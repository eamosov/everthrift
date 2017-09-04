package org.everthrift.appserver.scheduling.annotation;

import org.everthrift.appserver.scheduling.DistributedTaskScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.config.DestructionAwareBeanPostProcessor;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.scheduling.support.ScheduledMethodRunnable;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.MethodCallback;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

public class DistributedScheduledAnnotationBeanPostProcessor implements DestructionAwareBeanPostProcessor, Ordered, EmbeddedValueResolverAware, DisposableBean {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private DistributedTaskScheduler scheduler;

    private StringValueResolver embeddedValueResolver;

    private final Set<Class<?>> nonAnnotatedClasses = Collections.newSetFromMap(new ConcurrentHashMap<Class<?>, Boolean>(64));

    private final Map<Object, Set<ScheduledFuture<?>>> scheduledTasks = new ConcurrentHashMap<Object, Set<ScheduledFuture<?>>>(16);

    public DistributedScheduledAnnotationBeanPostProcessor() {

    }

    public DistributedScheduledAnnotationBeanPostProcessor(DistributedTaskScheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }

    public void setScheduler(DistributedTaskScheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public void setEmbeddedValueResolver(StringValueResolver resolver) {
        this.embeddedValueResolver = resolver;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(final Object bean, String beanName) {

        Assert.notNull(scheduler, "DistributedTaskScheduler must be set");

        Class<?> targetClass = AopUtils.getTargetClass(bean);
        if (!this.nonAnnotatedClasses.contains(targetClass)) {
            final Set<Method> annotatedMethods = new LinkedHashSet<Method>(1);
            ReflectionUtils.doWithMethods(targetClass, new MethodCallback() {
                @Override
                public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
                    for (DistributedScheduled scheduled :
                        AnnotationUtils.getRepeatableAnnotation(method, DistributedSchedules.class, DistributedScheduled.class)) {
                        processScheduled(scheduled, method, bean, beanName);
                        annotatedMethods.add(method);
                    }
                }
            });
            if (annotatedMethods.isEmpty()) {
                this.nonAnnotatedClasses.add(targetClass);
                if (logger.isDebugEnabled()) {
                    logger.debug("No @DistributedScheduled annotations found on bean class: " + bean.getClass());
                }
            } else {
                // Non-empty set of methods
                if (logger.isDebugEnabled()) {
                    logger.debug(annotatedMethods.size() + " @DistributedScheduled methods processed on bean '" + beanName +
                                     "': " + annotatedMethods);
                }
            }
        }
        return bean;
    }

    protected void processScheduled(DistributedScheduled scheduled, Method method, Object bean, String beanName) {
        try {
            Assert.isTrue(method.getParameterTypes().length == 0, "Only no-arg methods may be annotated with @DistributedScheduled");

            final String name;

            if (scheduled.name().isEmpty()) {
                name = beanName + ":" + method.getName();
            } else {
                name = scheduled.name();
            }

            if (AopUtils.isJdkDynamicProxy(bean)) {
                try {
                    // Found a @Scheduled method on the target class for this JDK proxy ->
                    // is it also present on the proxy itself?
                    method = bean.getClass().getMethod(method.getName(), method.getParameterTypes());
                } catch (SecurityException ex) {
                    ReflectionUtils.handleReflectionException(ex);
                } catch (NoSuchMethodException ex) {
                    throw new IllegalStateException(String.format(
                        "@Scheduled method '%s' found on bean target class '%s' but not " +
                            "found in any interface(s) for a dynamic proxy. Either pull the " +
                            "method up to a declared interface or switch to subclass (CGLIB) " +
                            "proxies by setting proxy-target-class/proxyTargetClass to 'true'",
                        method.getName(), method.getDeclaringClass().getSimpleName()));
                }
            } else if (AopUtils.isCglibProxy(bean)) {
                // Common problem: private methods end up in the proxy instance, not getting delegated.
                if (Modifier.isPrivate(method.getModifiers())) {
                    throw new IllegalStateException(String.format(
                        "@Scheduled method '%s' found on CGLIB proxy for target class '%s' but cannot " +
                            "be delegated to target bean. Switch its visibility to package or protected.",
                        method.getName(), method.getDeclaringClass().getSimpleName()));
                }
            }


            final Runnable runnable = new ScheduledMethodRunnable(bean, method);
            boolean processedSchedule = false;
            final String errorMessage = "Exactly one of the 'cron', 'fixedDelay(String)', or 'fixedRate(String)' attributes is required";

            Set<ScheduledFuture<?>> tasks = this.scheduledTasks.get(bean);
            if (tasks == null) {
                tasks = new LinkedHashSet<ScheduledFuture<?>>(4);
                this.scheduledTasks.put(bean, tasks);
            }

            // Determine initial delay
            long initialDelay = scheduled.initialDelay();
            String initialDelayString = scheduled.initialDelayString();
            if (StringUtils.hasText(initialDelayString)) {
                Assert.isTrue(initialDelay < 0, "Specify 'initialDelay' or 'initialDelayString', not both");
                if (this.embeddedValueResolver != null) {
                    initialDelayString = this.embeddedValueResolver.resolveStringValue(initialDelayString);
                }
                try {
                    initialDelay = Long.parseLong(initialDelayString);
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("Invalid initialDelayString value \"" + initialDelayString + "\" - cannot parse into integer");
                }
            }

            // Check cron expression
            String cron = scheduled.cron();
            if (StringUtils.hasText(cron)) {
                Assert.isTrue(initialDelay == -1, "'initialDelay' not supported for cron triggers");
                processedSchedule = true;
                String zone = scheduled.zone();
                if (this.embeddedValueResolver != null) {
                    cron = this.embeddedValueResolver.resolveStringValue(cron);
                    zone = this.embeddedValueResolver.resolveStringValue(zone);
                }
                TimeZone timeZone;
                if (StringUtils.hasText(zone)) {
                    timeZone = StringUtils.parseTimeZoneString(zone);
                } else {
                    timeZone = TimeZone.getDefault();
                }
                tasks.add(scheduler.schedule(name, runnable, new CronTrigger(cron, timeZone)));
            }

            // At this point we don't need to differentiate between initial
            // delay set or not anymore
            if (initialDelay < 0) {
                initialDelay = 0;
            }

            // Check fixed delay
            long fixedDelay = scheduled.fixedDelay();
            if (fixedDelay >= 0) {
                Assert.isTrue(!processedSchedule, errorMessage);
                processedSchedule = true;

                if (initialDelay > 0) {
                    tasks.add(scheduler.scheduleWithFixedDelay(name, runnable, new Date(System.currentTimeMillis() + initialDelay), fixedDelay));
                } else {
                    tasks.add(scheduler.scheduleWithFixedDelay(name, runnable, fixedDelay));
                }
            }
            String fixedDelayString = scheduled.fixedDelayString();
            if (StringUtils.hasText(fixedDelayString)) {
                Assert.isTrue(!processedSchedule, errorMessage);
                processedSchedule = true;
                if (this.embeddedValueResolver != null) {
                    fixedDelayString = this.embeddedValueResolver.resolveStringValue(fixedDelayString);
                }
                try {
                    fixedDelay = Long.parseLong(fixedDelayString);
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("Invalid fixedDelayString value \"" + fixedDelayString + "\" - cannot parse into integer");
                }

                if (initialDelay > 0) {
                    tasks.add(scheduler.scheduleWithFixedDelay(name, runnable, new Date(System.currentTimeMillis() + initialDelay), fixedDelay));
                } else {
                    tasks.add(scheduler.scheduleWithFixedDelay(name, runnable, fixedDelay));
                }

            }

            // Check fixed rate
            long fixedRate = scheduled.fixedRate();
            if (fixedRate >= 0) {
                Assert.isTrue(!processedSchedule, errorMessage);
                processedSchedule = true;
                if (initialDelay > 0) {
                    tasks.add(scheduler.scheduleAtFixedRate(name, runnable, new Date(System.currentTimeMillis() + initialDelay), fixedRate));
                } else {
                    tasks.add(scheduler.scheduleAtFixedRate(name, runnable, fixedRate));
                }
            }
            String fixedRateString = scheduled.fixedRateString();
            if (StringUtils.hasText(fixedRateString)) {
                Assert.isTrue(!processedSchedule, errorMessage);
                processedSchedule = true;
                if (this.embeddedValueResolver != null) {
                    fixedRateString = this.embeddedValueResolver.resolveStringValue(fixedRateString);
                }
                try {
                    fixedRate = Long.parseLong(fixedRateString);
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("Invalid fixedRateString value \"" + fixedRateString + "\" - cannot parse into integer");
                }

                if (initialDelay > 0) {
                    tasks.add(scheduler.scheduleAtFixedRate(name, runnable, new Date(System.currentTimeMillis() + initialDelay), fixedRate));
                } else {
                    tasks.add(scheduler.scheduleAtFixedRate(name, runnable, fixedRate));
                }

            }

            // Check whether we had any attribute set
            Assert.isTrue(processedSchedule, errorMessage);
        } catch (IllegalArgumentException ex) {
            throw new IllegalStateException("Encountered invalid @DistributedScheduled method '" + method.getName() + "': " + ex
                .getMessage());
        }
    }

    @Override
    public void postProcessBeforeDestruction(Object bean, String beanName) {
        Set<ScheduledFuture<?>> tasks = this.scheduledTasks.remove(bean);
        if (tasks != null) {
            for (ScheduledFuture<?> task : tasks) {
                task.cancel(true);
            }
        }
    }

    @Override
    public boolean requiresDestruction(Object bean) {
        return this.scheduledTasks.containsKey(bean);
    }

    @Override
    public void destroy() {
        Collection<Set<ScheduledFuture<?>>> allTasks = this.scheduledTasks.values();
        for (Set<ScheduledFuture<?>> tasks : allTasks) {
            for (ScheduledFuture<?> task : tasks) {
                task.cancel(true);
            }
        }
        this.scheduledTasks.clear();
    }
}

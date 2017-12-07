package org.everthrift.appserver.controller;

import com.google.common.collect.ImmutableSet;
import org.apache.thrift.TBase;
import org.everthrift.appserver.BeanDefinitionHolder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.core.type.StandardMethodMetadata;
import org.springframework.util.ClassUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ThriftControllerRegistry implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(ThriftControllerRegistry.class);

    @Autowired
    protected ApplicationContext applicationContext;

    @Autowired
    private BeanDefinitionHolder beanDefinitionHolder;

    @NotNull
    private Map<String, ThriftControllerInfo> map = Collections.synchronizedMap(new HashMap<String, ThriftControllerInfo>());

    @NotNull
    private List<Class<ConnectionStateHandler>> stateHandlers = new CopyOnWriteArrayList<>();

    private final Class<? extends Annotation> annotationType;

    public ThriftControllerRegistry(Class<? extends Annotation> annotationType) {
        this.annotationType = annotationType;
    }

    public Class<? extends Annotation> getType() {
        return annotationType;
    }

    private void scanThriftControllers(@NotNull Class<? extends Annotation> annotationType) {

        for (String beanName : ImmutableSet.copyOf(beanDefinitionHolder.getBeanDefinitionRegistry()
                                                                       .getBeanDefinitionNames())) {
            final BeanDefinition beanDefinition = beanDefinitionHolder.getBeanDefinitionRegistry()
                                                                      .getBeanDefinition(beanName);

            if (beanDefinition.isPrototype()) {

                Class beanCls = null;
                if (beanDefinition.getBeanClassName() != null) {
                    try {
                        beanCls = Class.forName(beanDefinition.getBeanClassName());
                    } catch (ClassNotFoundException e) {
                    }
                } else if (beanDefinition.getSource() instanceof StandardMethodMetadata) {
                    beanCls = ((StandardMethodMetadata) beanDefinition.getSource()).getIntrospectedMethod()
                                                                                   .getReturnType();
                }

                if (beanCls != null) {
                    if (ThriftController.class.isAssignableFrom(beanCls) && beanCls.getAnnotation(annotationType) != null) {
                        registerController(beanName, beanCls);
                    } else if (ConnectionStateHandler.class.isAssignableFrom(beanCls)) {
                        stateHandlers.add((Class<ConnectionStateHandler>) beanCls);
                    }
                }
            }
        }
    }

    private Class getArgsCls(@NotNull Class cls) {
        for (Method m : cls.getMethods()) {
            if (m.getName().equals("setup") && m.getParameterTypes().length == 1 && !m.isBridge()) {
                return m.getParameterTypes()[0];
            }
        }
        return null;
    }

    private ThriftControllerInfo registerController(String beanName, @NotNull Class cls) {

        final Class argsCls = getArgsCls(cls);

        if (argsCls == null) {
            log.error("Could't extract arguments' class from {}", cls.getCanonicalName());
            return null;
        }

        return registerController(beanName, cls, argsCls);
    }

    @Nullable
    public ThriftControllerInfo registerController(String beanName, @NotNull Class ctrlCls, @NotNull Class argsCls) {
        final ThriftControllerInfo i = tryRegisterController(beanName, ctrlCls, argsCls);
        log.debug("registerController: {}", i.getBeanName());
        map.put(i.getName(), i);
        return i;
    }

    public ThriftControllerInfo getController(String name) {
        return map.get(name);
    }

    @NotNull
    public Set<String> getContollerNames() {
        return map.keySet();
    }

    @NotNull
    public Map<String, ThriftControllerInfo> getControllers() {
        return map;
    }

    @Nullable
    private ThriftControllerInfo tryRegisterController(@Nullable final String beanName, @NotNull final Class cls, @NotNull Class argument) {

        if (!TBase.class.isAssignableFrom(argument)) {
            log.debug("Result class {} is not TBase", argument.getSimpleName());
            throw new IllegalArgumentException();
        }

        final Pattern serviceArgs = Pattern.compile("(.*)\\.([a-zA-Z0-9_-]+)\\.([a-zA-Z0-9_-]+)_args");
        final Matcher m = serviceArgs.matcher(argument.getCanonicalName());

        if (!m.matches()) {
            log.trace("Argument '{}' not matches pattern", argument.getCanonicalName());
            throw new IllegalArgumentException(String.format("Argument '%s' not matches pattern, cls=%s", argument.getCanonicalName(), cls
                .getCanonicalName()));
        }

        final String service = m.group(2);
        final String method = m.group(3);
        final Class<?> resultWrap = ClassUtils.resolveClassName(m.group(1) + "." + m.group(2) + "." + m.group(3) + "_result",
                                                                ClassUtils.getDefaultClassLoader());

        Method findResultFieldByName = null;

        for (Class i : resultWrap.getDeclaredClasses()) {
            if (i.getCanonicalName().endsWith("_Fields")) {

                try {
                    findResultFieldByName = i.getMethod("findByName", String.class);
                } catch (SecurityException e) {
                    throw new IllegalArgumentException();
                } catch (NoSuchMethodException e) {
                    throw new IllegalArgumentException();
                }

                break;
            }
        }

        if (findResultFieldByName == null) {
            log.error("Coudn't find {}._Fields", resultWrap.getCanonicalName());
            throw new IllegalArgumentException();
        }

        final ThriftControllerInfo i = new ThriftControllerInfo(applicationContext,
                                                                beanName != null ? beanName : service + ":" + method,
                                                                (Class<? extends ThriftController>) cls,
                                                                service,
                                                                method,
                                                                (Class<? extends TBase>) argument,
                                                                (Class<? extends TBase>) resultWrap,
                                                                findResultFieldByName);
        return i;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        scanThriftControllers(annotationType);
    }

    @NotNull
    public List<Class<ConnectionStateHandler>> getStateHandlers() {
        return stateHandlers;
    }

}

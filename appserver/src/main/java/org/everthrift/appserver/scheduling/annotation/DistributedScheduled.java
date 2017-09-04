package org.everthrift.appserver.scheduling.annotation;

import org.springframework.scheduling.TriggerContext;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation that marks a method to be scheduled. Exactly one of
 * the {@link #cron()}, {@link #fixedDelay()}, or {@link #fixedRate()}
 * attributes must be specified.
 * <p>
 * <p>The annotated method must expect no arguments. It will typically have
 * a {@code void} return type; if not, the returned value will be ignored
 * when called through the scheduler.
 * <p>
 * <p>Processing of {@code @DistributedScheduled} annotations is performed by
 * registering a {@link DistributedTaskScheduler}.
 * <p>
 * <p>This annotation may be used as a <em>meta-annotation</em> to create custom
 * <em>composed annotations</em> with attribute overrides.
 *
 * @author Mark Fisher
 * @author Dave Syer
 * @author Chris Beams
 * @see EnableScheduling
 * @see DistributedScheduledAnnotationBeanPostProcessor
 * @see DistributedSchedules
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Repeatable(DistributedSchedules.class)
public @interface DistributedScheduled {

    String name() default "";

    /**
     * A cron-like expression, extending the usual UN*X definition to include
     * triggers on the second as well as minute, hour, day of month, month
     * and day of week.  e.g. {@code "0 * * * * MON-FRI"} means once per minute on
     * weekdays (at the top of the minute - the 0th second).
     *
     * @return an expression that can be parsed to a cron schedule
     * @see org.springframework.scheduling.support.CronSequenceGenerator
     */
    String cron() default "";

    /**
     * A time zone for which the cron expression will be resolved. By default, this
     * attribute is the empty String (i.e. the server's local time zone will be used).
     *
     * @return a zone id accepted by {@link java.util.TimeZone#getTimeZone(String)},
     * or an empty String to indicate the server's default time zone
     * @see org.springframework.scheduling.support.CronTrigger#CronTrigger(String, java.util.TimeZone)
     * @see java.util.TimeZone
     */
    String zone() default "";

    /**
     * Execute the annotated method with a fixed period in milliseconds between the
     * end of the last invocation and the start of the next.
     * <p>
     * Этот режим старается сохранить фиксированное время между следующим планированием и
     * предыдущим актуальным временем выполнения задачи, т.е. next.lastScheduledExecutionTime - prev.lastActualExecutionTime()
     *
     * @return the delay in milliseconds
     */
    long fixedDelay() default -1;

    /**
     * Execute the annotated method with a fixed period in milliseconds between the
     * end of the last invocation and the start of the next.
     *
     * @return the delay in milliseconds as a String value, e.g. a placeholder
     */
    String fixedDelayString() default "";

    /**
     * Execute the annotated method with a fixed period in milliseconds between
     * invocations.
     * <p>
     * Этот режим старается сохранить фиксированное время между двумя соседними планированиями т.е. next.lastScheduledExecutionTime - prev.lastScheduledExecutionTime
     *
     * @return the period in milliseconds
     */
    long fixedRate() default -1;

    /**
     * Execute the annotated method with a fixed period in milliseconds between
     * invocations.
     *
     * @return the period in milliseconds as a String value, e.g. a placeholder
     */
    String fixedRateString() default "";

    /**
     * Number of milliseconds to delay before the first execution of a
     * {@link #fixedRate()} or {@link #fixedDelay()} task.
     *
     * @return the initial delay in milliseconds
     */
    long initialDelay() default -1;

    /**
     * Number of milliseconds to delay before the first execution of a
     * {@link #fixedRate()} or {@link #fixedDelay()} task.
     *
     * @return the initial delay in milliseconds as a String value, e.g. a placeholder
     */
    String initialDelayString() default "";

    /**
     * For accessing TriggerContext from Runnable tasks
     */
    ThreadLocal<TriggerContext> triggerContext = new ThreadLocal<>();
}

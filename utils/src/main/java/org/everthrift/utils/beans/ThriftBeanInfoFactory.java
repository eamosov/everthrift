package org.everthrift.utils.beans;

import org.apache.thrift.TBase;
import org.springframework.beans.BeanInfoFactory;
import org.springframework.beans.CachedIntrospectionResults;
import org.springframework.core.Ordered;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;

/**
 * {@link BeanInfoFactory} implementation that evaluates whether bean classes have
 * "non-standard" JavaBeans setter methods and are thus candidates for introspection
 * by Spring's (package-visible) {@code ExtendedBeanInfo} implementation.
 * <p>
 * <p>Ordered at {@link Ordered#LOWEST_PRECEDENCE} to allow other user-defined
 * {@link BeanInfoFactory} types to take precedence.
 *
 * @author Chris Beams
 * @see BeanInfoFactory
 * @see CachedIntrospectionResults
 * @since 3.2
 */
public class ThriftBeanInfoFactory implements BeanInfoFactory, Ordered {

    /**
     * Return an {@link ThriftBeanInfo} for the given bean class, if applicable.
     */
    @Override
    public BeanInfo getBeanInfo(Class<?> beanClass) throws IntrospectionException {
        return (supports(beanClass) ? new ThriftBeanInfo(Introspector.getBeanInfo(beanClass)) : null);
    }

    /**
     * Return whether the given bean class declares or inherits any non-void
     * returning bean property or indexed property setter methods.
     */
    private boolean supports(Class<?> beanClass) {
        return TBase.class.isAssignableFrom(beanClass);
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

}

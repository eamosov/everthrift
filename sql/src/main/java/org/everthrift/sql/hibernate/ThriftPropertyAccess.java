package org.everthrift.sql.hibernate;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.Map;

import org.hibernate.HibernateException;
import org.hibernate.PropertyNotFoundException;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.property.access.spi.Getter;
import org.hibernate.property.access.spi.PropertyAccess;
import org.hibernate.property.access.spi.PropertyAccessStrategy;
import org.hibernate.property.access.spi.Setter;
import org.springframework.beans.BeanUtils;

public class ThriftPropertyAccess implements PropertyAccess {

    public static class ThriftSetter implements Setter {
        private final Method writeMethod;

        private ThriftSetter(PropertyDescriptor pd) {
            this.writeMethod = pd.getWriteMethod();
        }

        @Override
        public void set(Object target, Object value, SessionFactoryImplementor factory) throws HibernateException {
            try {
                writeMethod.invoke(target, value);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new HibernateException(e);
            }
        }

        @Override
        public Method getMethod() {
            return null;
        }

        @Override
        public String getMethodName() {
            return null;
        }

        @Override
        public String toString() {
            return "ThriftSetter(" + writeMethod.getName() + ')';
        }
    }

    public static final class ThriftGetter implements Getter {
        private final Method readMethod;

        private ThriftGetter(PropertyDescriptor pd) {
            this.readMethod = pd.getReadMethod();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Object get(Object target) throws HibernateException {
            try {
                return readMethod.invoke(target);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new HibernateException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Object getForInsert(Object target, Map mergeMap, SessionImplementor session) {
            return get( target );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Class getReturnType() {
            return readMethod.getReturnType();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Member getMember() {
            return null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Method getMethod() {
            return null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getMethodName() {
            return null;
        }

        @Override
        public String toString() {
            return "BasicGetter(" + readMethod.getName() + ')';
        }

    }

    private final Setter setter;
    private final Getter getter;
    private PropertyAccessStrategy strategy;

    public ThriftPropertyAccess(PropertyAccessStrategy strategy, Class theClass, String propertyName) {
        super();
        this.strategy = strategy;

        final PropertyDescriptor pd = BeanUtils.getPropertyDescriptor(theClass, propertyName);
        if (pd == null)
            throw new PropertyNotFoundException("property " + propertyName + " not found in class" + theClass.getCanonicalName());

        this.setter = new ThriftSetter(pd);
        this.getter = new ThriftGetter(pd);
    }

    @Override
    public Setter getSetter() throws PropertyNotFoundException {
        return setter;
    }

    @Override
    public Getter getGetter() throws PropertyNotFoundException {
        return getter;
    }

    @Override
    public PropertyAccessStrategy getPropertyAccessStrategy() {
        return strategy;
    }

}
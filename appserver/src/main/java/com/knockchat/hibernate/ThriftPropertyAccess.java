package com.knockchat.hibernate;

import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.Map;

import org.hibernate.HibernateException;
import org.hibernate.PropertyNotFoundException;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.property.access.spi.Getter;
import org.hibernate.property.access.spi.PropertyAccess;
import org.hibernate.property.access.spi.PropertyAccessStrategy;
import org.hibernate.property.access.spi.Setter;

import com.google.common.base.CaseFormat;
import com.knockchat.hibernate.model.MetaDataProvider;
import com.knockchat.utils.meta.MetaClass;
import com.knockchat.utils.meta.MetaClasses;
import com.knockchat.utils.meta.MetaProperty;

public class ThriftPropertyAccess implements PropertyAccess {
	
    public static class ThriftSetter implements Setter {
        private Class clazz;
        private final transient MetaProperty property;
        private final String propertyName;

        private ThriftSetter(Class clazz, MetaProperty metaProperty, String propertyName) {
            this.clazz=clazz;
            this.property = metaProperty;
            this.propertyName=propertyName;
        }

        @Override
		public void set(Object target, Object value, SessionFactoryImplementor factory) throws HibernateException {
                property.set(target, value);
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
            return "ThriftSetter(" + clazz.getName() + '.' + propertyName + ')';
        }
    }

    public static final class ThriftStringAuxFieldSetter extends ThriftSetter {

        private final transient MetaProperty auxProperty;
        private final String auxPropertyName; //строковое поле

        private ThriftStringAuxFieldSetter(Class clazz, MetaProperty metaProperty, MetaProperty auxMetaProperty, String propertyName, String auxPropertyName) {
            super(clazz, metaProperty, propertyName);
            this.auxProperty = auxMetaProperty;
            this.auxPropertyName = auxPropertyName;
        }

        @Override
		public void set(Object target, Object value, SessionFactoryImplementor factory) throws HibernateException {
            super.set(target,value, factory);
            auxProperty.set(target, value.toString());
        }

        @Override
        public String toString() {
            return "ThriftStringAuxFieldSetter{" +
                    "auxProperty=" + auxProperty +
                    ", auxPropertyName='" + auxPropertyName + '\'' +
                    "} extends " + super.toString() ;
        }
    }


    public static final class ThriftGetter implements Getter {
        private Class clazz;
        private final transient MetaProperty property;
        private final String propertyName;

        private ThriftGetter(Class clazz, MetaProperty property, String propertyName) {
            this.clazz=clazz;
            this.property = property;
            this.propertyName=propertyName;
        }

        /**
         * {@inheritDoc}
         */
        @Override
		public Object get(Object target) throws HibernateException {
            return property.get(target);
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
            return property.getType();
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
            return "BasicGetter(" + clazz.getName() + '.' + propertyName + ')';
        }

    }
    
	private final Setter setter;
	private final Getter getter;
	private PropertyAccessStrategy strategy;

    public ThriftPropertyAccess(PropertyAccessStrategy strategy, Class theClass, String propertyName) {
		super();
		this.strategy = strategy;
		this.setter = createSetter(theClass, propertyName);
		this.getter = createGetter(theClass, propertyName);
	}

	@Override
	public Setter getSetter() throws PropertyNotFoundException {
        return setter;
    }

    private static Setter createSetter(Class theClass, String propertyName) throws PropertyNotFoundException {
    	
        ThriftSetter result = getSetterOrNull(theClass, propertyName);
        if (result==null) 
            throw new PropertyNotFoundException( "Could not find a setter for property " + propertyName + " in class " + theClass.getName() );
        
        return result;
    }

    private static ThriftSetter getSetterOrNull(Class theClass, String propertyName) {

        if (theClass==Object.class || theClass==null) return null;

        MetaClass metaClass = MetaClasses.get(theClass);
        if (metaClass == null) return null;
        MetaProperty property = metaClass.getProperty(propertyName);
        if (property!=null) {
            MetaProperty auxProperty = findAuxProperty(metaClass, property);
            if (auxProperty != null)
                return new ThriftStringAuxFieldSetter(theClass,property, auxProperty, propertyName, auxProperty.getName());
            return new ThriftSetter(theClass, property, propertyName);
        }
        else {
            return null;
        }
    }

    @Override
	public Getter getGetter() throws PropertyNotFoundException {
        return getter;
    }

    public static Getter createGetter(Class theClass, String propertyName) throws PropertyNotFoundException {
        final ThriftGetter result = getGetterOrNull(theClass, propertyName);
        
        if (result==null)
            throw new PropertyNotFoundException( "Could not find a getter for " + propertyName + " in class " + theClass.getName());
        
        return result;
    }

    private static ThriftGetter getGetterOrNull(Class theClass, String propertyName) {
        if (theClass==Object.class || theClass==null) {
            return null;
        }

        MetaClass metaClass = MetaClasses.get(theClass);
        if (metaClass == null) return null;
        MetaProperty property = metaClass.getProperty(propertyName);
        if (property!=null) {
            MetaProperty auxProperty = findAuxProperty(metaClass, property);
            return new ThriftGetter(theClass, auxProperty!=null?auxProperty:property, propertyName);
        } else {
            return null;
        }
    }

    private static MetaProperty findAuxProperty(MetaClass originClass ,MetaProperty originProperty){
        MetaProperty auxProperty = null;
        String auxProperyName = null;
        String auxLowerUderscoreProperyName = null;
        String propertyName = originProperty.getName();
        if (Date.class.isAssignableFrom(originProperty.getType())){
            auxProperyName = propertyName.substring(0,propertyName.length() - MetaDataProvider.TS_POSTFIX.length());
            auxLowerUderscoreProperyName = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, auxProperyName);
            auxProperty = originClass.getProperty(auxProperyName)!=null?originClass.getProperty(auxProperyName):originClass.getProperty(auxLowerUderscoreProperyName);
        }
        return auxProperty;
    }

	@Override
	public PropertyAccessStrategy getPropertyAccessStrategy() {
		return strategy;
	}

}
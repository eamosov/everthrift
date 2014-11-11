package com.knockchat.utils.meta;

import java.util.Arrays;

import com.knockchat.utils.JvmNotation;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public abstract class AbstractMetaClass implements MetaClass {

	protected final Class<?> objectClass;

	protected final String jvmName;
	protected final String jvmType;

	public AbstractMetaClass(Class<?> objectClass) {
		this.objectClass = objectClass;
		this.jvmName = JvmNotation.getJvmClass( objectClass );
		this.jvmType = JvmNotation.getJvmType( objectClass );
	}

	@Override
	public Class<?> getObjectClass() {
		return objectClass;
	}

	@Override
	public MetaClass getNested( String name ) {
		try {
			ClassLoader cl = objectClass.getClassLoader();
			
			if ( cl == null )
				cl = ClassLoader.getSystemClassLoader();
			
			return MetaClasses.get( cl.loadClass( objectClass.getName() + "$" + name ) );
		} catch ( ClassNotFoundException e ) {
			return null;
		}
	}

	@Override
	public Object newInstance() {
		try {
			return objectClass.newInstance();
		} catch ( Throwable e ) {
			throw new Error( e );
		}
	}

	@Override
	public String getName() {
		return objectClass.getName();
	}

	@Override
	public String getJvmName() {
		return jvmName;
	}

	@Override
	public String getJvmType() {
		return jvmType;
	}

	@Override
	public MetaProperty getProperty( String propertyName ) {
		MetaProperty prop = getBeanProperty( propertyName );

		if ( prop != null )
			return prop;

		return getFieldProperty( propertyName );
	}

	@Override
	public MetaProperty[] getProperties() {
		MetaProperty[] fieldProps = getFieldProperties();
		MetaProperty[] beanProps = getBeanProperties();

		if ( beanProps.length == 0 )
			return fieldProps;

		MetaProperty[] props = Arrays.copyOf( fieldProps, fieldProps.length + beanProps.length );

		for ( int i = 0; i < beanProps.length; ++ i )
			props[fieldProps.length + i] = beanProps[i];

		return props;
	}
}
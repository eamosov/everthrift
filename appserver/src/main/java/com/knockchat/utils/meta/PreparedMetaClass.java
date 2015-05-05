package com.knockchat.utils.meta;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public abstract class PreparedMetaClass extends AbstractMetaClass {

	protected final Map<String,MetaProperty> fieldProperties = new LinkedHashMap<String,MetaProperty>();
	protected final Map<String,MetaProperty> beanProperties = new LinkedHashMap<String,MetaProperty>();
	protected final Map<String,MetaMethod> methods = new LinkedHashMap<String,MetaMethod>();

	public PreparedMetaClass( Class<?> objectClass ) {
		super( objectClass );
	}

	@Override
	public MetaProperty getBeanProperty( String propertyName ) {
		return beanProperties.get( propertyName );
	}

	@Override
	public MetaProperty getFieldProperty( String fieldName ) {
		return fieldProperties.get( fieldName );
	}

	@Override
	public MetaMethod getMethod( String methodName ) {
		return methods.get( methodName );
	}
	
	@Override
	public MetaMethod[] getMethods() {
		return methods.values().toArray( new MetaMethod[ methods.size() ] );
	}

	@Override
	public MetaProperty[] getBeanProperties() {
		return beanProperties.values().toArray( new MetaProperty[ beanProperties.size() ] );
	}

	@Override
	public MetaProperty[] getFieldProperties() {
		return fieldProperties.values().toArray( new MetaProperty[ fieldProperties.size() ] );
	}

}
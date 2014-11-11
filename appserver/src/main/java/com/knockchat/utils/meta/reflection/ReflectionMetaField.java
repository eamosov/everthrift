package com.knockchat.utils.meta.reflection;

import java.lang.reflect.Field;

import com.knockchat.utils.meta.MetaProperty;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class ReflectionMetaField implements MetaProperty {
	
	private final Field field;
	
	public ReflectionMetaField( Field field ) {
		this.field = field;
	}

	@Override
	public Object get( Object target ) {
		try {
			return field.get( target );
		} catch ( Throwable e ) {
			throw new Error( "Can't get field " + field.getDeclaringClass().getName() + "#" + field.getName() + " from " + target, e );
		}
	}

	@Override
	public void set( Object target, Object value ) {
		try {
			field.set( target, value );
		} catch ( Throwable e ) {
			throw new Error( "Can't set field " + field.getDeclaringClass().getName() + "#" + field.getName() + " at " + target, e );
		}
	}

	@Override
	public String getName() {
		return field.getName();
	}

	@Override
	public Class<?> getType() {
		return field.getType();
	}

}

package com.knockchat.utils.meta.reflection;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.knockchat.utils.meta.PreparedMetaClass;
import com.knockchat.utils.meta.getset.GetSetPropertySupport;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class ReflectionMetaClass extends PreparedMetaClass{
	
	@SuppressWarnings("unused")
	private static final Logger log = LoggerFactory.getLogger( ReflectionMetaClass.class );

	public ReflectionMetaClass( Class<?> objectClass ) {
		super( objectClass );
		
		for ( Field field : objectClass.getFields() ) {
			if ( ( field.getModifiers() & Modifier.STATIC) != 0 )
				continue;
			
			fieldProperties.put( field.getName(), new ReflectionMetaField(field) );
		}
		
		for ( Method method : objectClass.getMethods() ) {
			if ( ( method.getModifiers() & Modifier.STATIC) != 0 )
				continue;
			
			methods.put( method.getName(), new ReflectionMetaMethod(method) );
		}
		
		GetSetPropertySupport.get( this, beanProperties );
	}

}

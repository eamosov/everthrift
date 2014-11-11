package com.knockchat.utils.meta.reflection;

import java.lang.reflect.Method;
import java.util.Arrays;

import com.knockchat.utils.JvmNotation;
import com.knockchat.utils.meta.MetaMethod;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class ReflectionMetaMethod implements MetaMethod {	

	private static final Object[] NO_OBJECTS = new Object[0];
	
	private final Method method;
	
	private final String signature;
	
	public ReflectionMetaMethod( Method method ) {
		this.method = method;
		this.signature = JvmNotation.getJvmSignature( method );
	}

	@Override
	public Object invoke( Object target, Object... args ) {
		try {
			return method.invoke( target, adaptCallArguments( args ) );
		} catch ( Throwable e ) {
			throw new Error( "Can't invoke method " + method.getDeclaringClass().getName() + "#" + method.getName() + " on " + target, e );
		}
	}

	private Object[] adaptCallArguments( Object[] args ) {
		if ( method.getParameterTypes().length == 0 ) {
			return NO_OBJECTS;
		} else if ( method.getParameterTypes().length < args.length ) {
			return Arrays.copyOf( args, method.getParameterTypes().length );
		} else {
			return args;
		}
	}

	@Override
	public String getName() {
		return method.getName();
	}

	@Override
	public String getSignature() {
		return signature;
	}

	@Override
	public Class<?> getReturnType() {
		return method.getReturnType();
	}
}

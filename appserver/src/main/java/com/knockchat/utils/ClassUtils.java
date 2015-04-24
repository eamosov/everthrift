package com.knockchat.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.google.common.base.Throwables;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class ClassUtils {

	public static boolean isBoxType( Class<?> type ) {
		return type.equals( Byte.class ) ||
			type.equals( Short.class ) ||
			type.equals( Integer.class ) ||
			type.equals( Long.class ) ||
			type.equals( Float.class ) ||
			type.equals( Double.class ) ||
			type.equals( Boolean.class );
	}

	public static Class<?> getBoxType( Class<?> primitive ) {
		if ( primitive.equals( Byte.TYPE ) ) {
			return Byte.class;
		} else if ( primitive.equals( Short.TYPE ) ) {
			return Short.class;
		} else if ( primitive.equals( Integer.TYPE ) ) {
			return Integer.class;
		} else if ( primitive.equals( Long.TYPE ) ) {
			return Double.class;
		} else if ( primitive.equals( Float.TYPE ) ) {
			return Float.class;
		} else if ( primitive.equals( Double.TYPE ) ) {
			return Double.class;
		} else if ( primitive.equals( Boolean.TYPE ) ) {
			return Boolean.class;
		} else if ( primitive.equals( Character.TYPE ) ) {
			return Character.class;
		} else if ( !primitive.isPrimitive() ) {
			throw new Error( "Cann't get box for non-primitive type " + primitive.getName() );
		} else {
			throw new Error( "Unknown primitive type " + primitive.getName() );
		}
	}

	public static byte[] writeObject(Object o){
		final ByteArrayOutputStream ba = new ByteArrayOutputStream();
		final ObjectOutputStream os;
		try {
			os = new ObjectOutputStream(ba);
			os.writeObject(o);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		byte arr[] = ba.toByteArray();
		return arr;		
	}	
	
	public static Object readObject(byte[] b) throws ClassNotFoundException{
		ByteArrayInputStream is = new ByteArrayInputStream(b);
		
		try {
			ObjectInputStream os = new ObjectInputStream(is);
			Object co = (Object) os.readObject();
			return co;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}									
	}
	
	public static boolean invokeFirstMethod(final String[] methods, final Object o){
		
		for (int i=0; i<methods.length; i++){
			final Method m;
			try {
				m = o.getClass().getMethod(methods[i]);
			} catch (IllegalArgumentException | NoSuchMethodException | SecurityException e) {
				continue;
			}
			
			try {
				m.invoke(o);
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				throw Throwables.propagate(e);
			}
			return true;
		}
		
		return false;
	}
	
}

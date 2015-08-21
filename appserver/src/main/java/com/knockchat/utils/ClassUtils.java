package com.knockchat.utils;

import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Throwables;
import com.knockchat.appserver.model.Registry;


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
	
	private static final Object NULL_METHOD = new Object();
	private static final AtomicReference<Int2ReferenceMap<Object>> methods = new AtomicReference<Int2ReferenceMap<Object>>(new Int2ReferenceOpenHashMap<Object>());

	private static Method findFirstMethod(final String[] methods, final Class cls, Class... parameterTypes){
		for (int i=0; i<methods.length; i++){
			final Method m;
			try {
				m =cls.getMethod(methods[i], parameterTypes);
			} catch (IllegalArgumentException | NoSuchMethodException | SecurityException e) {
				continue;
			}
			return m;
		}
		return null;
	}

	public static boolean invokeFirstMethod(final String[] _methods, final Object o, final Registry r){
		
		Int2ReferenceMap<Object> methods = ClassUtils.methods.get();
		
		int k = 1;
		k = 31 * k + o.getClass().hashCode();
		k = 31 * k + Arrays.hashCode(_methods);
		
		Object m = methods.get(k);
		
		if (m == null){
			m = findFirstMethod(_methods, o.getClass(), Registry.class);
			if (m == null)
				m = NULL_METHOD;

			methods = new Int2ReferenceOpenHashMap<Object>(methods);
			methods.put(k, m);
			ClassUtils.methods.set(methods);
		}
		
		if (m == NULL_METHOD)
			return false;
		
		try {
			((Method)m).invoke(o, r);
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw Throwables.propagate(e);
		}
		return true;
	}
	
	@SuppressWarnings("rawtypes")
	public static Field getDeclaredField(Class cls, String fieldName) throws NoSuchFieldException, SecurityException{		
		try{
			return cls.getDeclaredField(fieldName);
		}catch(NoSuchFieldException e){
			final Class sc = cls.getSuperclass();
			if (sc != Object.class && sc !=null)
				return getDeclaredField(sc, fieldName);
			else
				throw e;			
		}
	}
	
	
}

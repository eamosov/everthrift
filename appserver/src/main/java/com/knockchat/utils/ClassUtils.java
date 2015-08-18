package com.knockchat.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;


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
	
	private static class Key{
		final String methods[];
		final Class cls;
		
		public Key(String[] methods, Class cls) {
			super();
			this.methods = methods;
			this.cls = cls;
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((cls == null) ? 0 : cls.hashCode());
			result = prime * result + Arrays.hashCode(methods);
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Key other = (Key) obj;
			if (cls == null) {
				if (other.cls != null)
					return false;
			} else if (!cls.equals(other.cls))
				return false;
			if (!Arrays.equals(methods, other.methods))
				return false;
			return true;
		}			
	}
	
	private static final Object NULL_METHOD = new Object();
	private static final AtomicReference<Map<Key, Object>> methods = new AtomicReference<Map<Key, Object>>(Maps.<Key, Object>newHashMap());

	private static Method findFirstMethod(final String[] methods, final Class cls){
		for (int i=0; i<methods.length; i++){
			final Method m;
			try {
				m =cls.getMethod(methods[i]);
			} catch (IllegalArgumentException | NoSuchMethodException | SecurityException e) {
				continue;
			}
			return m;
		}
		return null;
	}

	public static boolean invokeFirstMethod(final String[] _methods, final Object o){
		
		Map<Key, Object> methods = ClassUtils.methods.get();
		final Key k = new Key(_methods, o.getClass());
		
		Object m = methods.get(k);
		
		if (m == null){
			m = findFirstMethod(_methods, o.getClass());
			if (m == null)
				m = NULL_METHOD;

			methods = Maps.newHashMap(methods);
			methods.put(k, m);
			ClassUtils.methods.set(methods);
		}
		
		if (m == NULL_METHOD)
			return false;
		
		try {
			((Method)m).invoke(o);
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

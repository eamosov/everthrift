package org.everthrift.appserver.utils;

import java.beans.PropertyDescriptor;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.Map;

import org.jgroups.util.ArrayIterator;
import org.springframework.beans.BeanUtils;

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
	
	public static Map<String, PropertyDescriptor> getPropertyDescriptors(Class cls){
		return Maps.uniqueIndex(new ArrayIterator(BeanUtils.getPropertyDescriptors(cls)), PropertyDescriptor::getName);
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

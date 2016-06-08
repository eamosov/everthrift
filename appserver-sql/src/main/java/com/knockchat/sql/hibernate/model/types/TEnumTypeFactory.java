package com.knockchat.sql.hibernate.model.types;

import java.util.Map;

import org.apache.thrift.TEnum;

import com.google.common.collect.Maps;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

@SuppressWarnings("rawtypes")
public class TEnumTypeFactory {
		
	private static final Map<Class<? extends TEnum>, Class<? extends TEnumType>> map = Maps.newIdentityHashMap();
	
	public static synchronized Class<? extends TEnumType> create(Class<? extends TEnum> enumType){
		
		Class<? extends TEnumType> cls = map.get(enumType);
		if (cls !=null)
			return cls;
		
		cls = getTEnumType(enumType);
		map.put(enumType, cls);
		return cls;
	}
	
	@SuppressWarnings("unchecked")
	private static Class<? extends TEnumType> getTEnumType(Class<? extends TEnum> enumType){		
		final ClassPool pool = ClassPool.getDefault();
		final CtClass cc = pool.makeClass("com.knockchat.hibernate.model.types.generated." + enumType.getSimpleName() + "EnumType");		
		try {
			cc.setSuperclass(pool.get(TEnumType.class.getName()));
			cc.addMethod(CtMethod.make("protected Class getTEnumClass() { try { return (Class)(Thread.currentThread().getContextClassLoader().loadClass(\"" + enumType.getName() + "\")); } catch (ClassNotFoundException e) {throw new RuntimeException(e);}}", cc));
			return cc.toClass();
		} catch (CannotCompileException | NotFoundException e) {
			throw new RuntimeException(e);
		}						
	}
}

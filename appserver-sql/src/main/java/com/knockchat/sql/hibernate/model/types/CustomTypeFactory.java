package com.knockchat.sql.hibernate.model.types;

import java.util.Map;

import org.hibernate.usertype.UserType;

import com.google.common.collect.Maps;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

@SuppressWarnings("rawtypes")
public class CustomTypeFactory {
		
	private static final Map<Class, Class<? extends UserType>> map = Maps.newIdentityHashMap();
	
	public static synchronized Class<? extends UserType> create(Class javaType, Class<? extends UserType> prototype){
		
		Class<? extends UserType> cls = map.get(javaType);
		if (cls !=null)
			return cls;
		
		cls = compile(javaType, prototype);
		map.put(javaType, cls);
		return cls;
	}
	
	@SuppressWarnings("unchecked")
	private static Class<? extends UserType> compile(Class javaType, Class<? extends UserType> prototype){		
		final ClassPool pool = ClassPool.getDefault();
		final CtClass cc = pool.makeClass(javaType.getCanonicalName() + "HibernateType");		
		try {
			cc.setSuperclass(pool.get(prototype.getName()));
			cc.addMethod(CtMethod.make("public Class returnedClass() { try { return (Class)(Thread.currentThread().getContextClassLoader().loadClass(\"" + javaType.getName() + "\")); } catch (ClassNotFoundException e) {throw new RuntimeException(e);}}", cc));
			return cc.toClass();
		} catch (CannotCompileException | NotFoundException e) {
			throw new RuntimeException(e);
		}						
	}
}

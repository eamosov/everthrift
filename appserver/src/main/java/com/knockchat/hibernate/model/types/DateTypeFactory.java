package com.knockchat.hibernate.model.types;

import java.util.Map;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

import com.google.common.collect.Maps;

@SuppressWarnings("rawtypes")
public class DateTypeFactory {
		
	private static final Map<Class, Class<? extends DateType>> map = Maps.newIdentityHashMap();
	
	public static synchronized Class<? extends DateType> create(Class dateType){
		
		Class<? extends DateType> cls = map.get(dateType);
		if (cls !=null)
			return cls;
		
		cls = compile(dateType);
		map.put(dateType, cls);
		return cls;
	}
	
	@SuppressWarnings("unchecked")
	private static Class<? extends DateType> compile(Class dateType){		
		final ClassPool pool = ClassPool.getDefault();
		final CtClass cc = pool.makeClass(dateType.getCanonicalName() + "HibernateType");		
		try {
			cc.setSuperclass(pool.get(DateType.class.getName()));
			cc.addMethod(CtMethod.make("public Class returnedClass() { try { return (Class)(Thread.currentThread().getContextClassLoader().loadClass(\"" + dateType.getName() + "\")); } catch (ClassNotFoundException e) {throw new RuntimeException(e);}}", cc));
			return cc.toClass();
		} catch (CannotCompileException | NotFoundException e) {
			throw new RuntimeException(e);
		}						
	}
}

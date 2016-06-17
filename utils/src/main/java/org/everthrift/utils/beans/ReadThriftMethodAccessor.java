package org.everthrift.utils.beans;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.thrift.TBase;

import sun.reflect.MethodAccessor;

class ReadThriftMethodAccessor extends ThriftMethodAccessor{
	
	static void patch(Class entityClass, String propertyName, Method readMethod) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException{
		
		final sun.reflect.MethodAccessor old = (MethodAccessor) getMethodAccessor.invoke(readMethod); 
		if (old !=null && old instanceof ThriftMethodAccessor)
			return;
		
		try {
			setMethodAccessor.invoke(readMethod, new ReadThriftMethodAccessor(entityClass, propertyName));
		} catch (NotThriftProperty e) {
		}			
	}

	private ReadThriftMethodAccessor(Class entityClass, String propertyName) throws NotThriftProperty {
		super(entityClass, propertyName);
	}

	@Override
	public Object invoke(Object arg0, Object[] arg1) throws IllegalArgumentException, InvocationTargetException {
		
		if (arg1.length !=0)
			throw new IllegalArgumentException();
		
		if (((TBase) arg0).isSet(fId))
			return ((TBase) arg0).getFieldValue(fId);

		return null;
	}
}

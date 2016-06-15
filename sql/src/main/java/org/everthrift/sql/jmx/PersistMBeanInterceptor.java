package org.everthrift.sql.jmx;

import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Enumeration;
import java.util.Properties;

import javax.management.Attribute;
import javax.management.AttributeList;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.jmx.export.annotation.ManagedResource;

public class PersistMBeanInterceptor implements MethodInterceptor{

	private final ApplicationPropertiesModelFactory propertiesModelFactory;

	private final Object clazzObj;
	private final String persistencaName;
	protected static final String GET_METHOD = "get";
	protected static final String IS_METHOD = "is";
	protected static final String SET_METHOD = "set";
	protected static final String TRUE = "true";
	protected static final String FALSE = "false";
	protected static final String DOUBLE_TYPE = "double";
	protected static final String INTEGER_TYPE = "int";
	private static final String SET_ATTRIBUTE = "setAttribute";
	private static final String SET_ATTRIBUTES = "setAttributes";

	public PersistMBeanInterceptor(Object obj, ApplicationPropertiesModelFactory propertiesModelFactory) {
		this.clazzObj = obj;
		this.persistencaName = this.clazzObj.getClass().getAnnotation(ManagedResource.class).persistName();
		this.propertiesModelFactory = propertiesModelFactory;
		
		this.load(clazzObj);
	}

	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		String method = invocation.getMethod().getName();
		Object[] args = invocation.getArguments();
		
		if (method.equals(SET_ATTRIBUTE) && args.length == 1 && args[0] instanceof Attribute) {
			
			Attribute attribute = (Attribute) (args[0]);
			this.storeProperty(attribute.getName(), "" + attribute.getValue());
			
		} else if (method.equals(SET_ATTRIBUTES) && args.length == 1 && args[0] instanceof AttributeList) {
			
			for (Object object : (AttributeList) (args[0])) {
				Attribute attribute = (Attribute) object;
				this.storeProperty(attribute.getName(), "" + attribute.getValue());
			}
		}
		return invocation.proceed();
	}

	private void load(Object clazz) {
		Properties props = loadProperties();
		Enumeration keyNames = props.propertyNames();
		while (keyNames.hasMoreElements()) {
			String propertyKey = (String) keyNames.nextElement();
			String propertyValue = (String) props.get(propertyKey);
			try {
				Class parameterType;
				Object[] invokeParam;
				Method methodCall;
				if (propertyValue.startsWith(TRUE) || propertyValue.startsWith(FALSE)) {
					parameterType = boolean.class;
					boolean booleanValue = Boolean.parseBoolean(propertyValue);
					invokeParam = new Object[] { booleanValue };
				} else {
					parameterType = clazz.getClass().getMethod(GET_METHOD + convertMethodName(propertyKey), null).getReturnType();
					invokeParam = new Object[] { convertValue(parameterType, propertyValue) };
				}
				Class[] parameterTypes = new Class[] { parameterType };
				methodCall = clazz.getClass().getMethod(SET_METHOD + convertMethodName(propertyKey), parameterTypes);
				methodCall.invoke(clazz, invokeParam);
			} catch (NoSuchMethodException nsme) {
			} catch (InvocationTargetException ite) {
				ite.printStackTrace();
			} catch (IllegalAccessException iae) {
				iae.printStackTrace();
			}
		}
	}

	private Object convertValue(Class<?> targetType, String text) {
		PropertyEditor editor = PropertyEditorManager.findEditor(targetType);
		editor.setAsText(text);
		return editor.getValue();
	}

	private String convertMethodName(String keyName) {
		String methodName = "";
		if (keyName.indexOf(".get") > -1) {
			methodName = keyName.substring(keyName.indexOf(".get"));
			methodName = methodName.substring(4);
		} else if (keyName.indexOf(".is") > -1) {
			methodName = keyName.substring(keyName.indexOf(".is"));
			methodName = methodName.substring(3);
		} else {
			methodName = keyName;
		}
		return methodName;
	}

	private Properties loadProperties() {
		Properties returnValue = new Properties();
		
		for (ApplicationPropertiesModel m : propertiesModelFactory.findByPersistanceName(persistencaName)){
			returnValue.put(m.getPropertyName(), m.getPropertyValue());
		}
		
		return returnValue;
	}

	private void storeProperty(String propertyName, String propertyValue) {
		
		final ApplicationPropertiesModel m = new ApplicationPropertiesModel();
		
		m.setId(persistencaName + "." + propertyName);
		m.setPersistanceName(persistencaName);
		m.setPropertyName(propertyName);
		m.setPropertyValue(propertyValue);
		
		propertiesModelFactory.updateEntity(m);
	}
}

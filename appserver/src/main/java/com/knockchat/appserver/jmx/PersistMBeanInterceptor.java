package com.knockchat.appserver.jmx;

import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Enumeration;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.management.Attribute;
import javax.management.AttributeList;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Component;

import com.knockchat.sql.objects.ObjectStatements;
import com.knockchat.sql.objects.QueryStatement;
import com.knockchat.sql.objects.UpdateStatement;

@Component(value = "persistMbeanInterceptor")
@Scope(value = "prototype")
public class PersistMBeanInterceptor implements MethodInterceptor, InitializingBean {

	@Autowired
	private StatementHolder holder;

	private Object clazzObj;
	private String persistenceName;
	protected static final String GET_METHOD = "get";
	protected static final String IS_METHOD = "is";
	protected static final String SET_METHOD = "set";
	protected static final String TRUE = "true";
	protected static final String FALSE = "false";
	protected static final String DOUBLE_TYPE = "double";
	protected static final String INTEGER_TYPE = "int";
	private static final String SET_ATTRIBUTE = "setAttribute";
	private static final String SET_ATTRIBUTES = "setAttributes";

	public PersistMBeanInterceptor(Object obj) {
		this.clazzObj = obj;
		this.persistenceName = this.clazzObj.getClass().getAnnotation(ManagedResource.class).persistName();
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
				nsme.printStackTrace();
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
		synchronized (holder.loadProperties) {
			returnValue.putAll(holder.loadProperties.queryMap(2, this.persistenceName));
		}
		return returnValue;
	}

	private void storeProperty(String propertyName, String propertyValue) {
		synchronized (holder.findProperty) {
			String value = holder.findProperty.queryFirst(this.persistenceName, propertyName);
			if (value != null) {
				synchronized (holder.updateProperty) {
					holder.updateProperty.update(null, propertyValue, this.persistenceName, propertyName);
				}
			} else {
				synchronized (holder.insertProperty) {
					holder.insertProperty.update(null, this.persistenceName, propertyName, propertyValue);
				}
			}
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		this.load(clazzObj);
	}

	@Component
	private static class StatementHolder {

		@Autowired
		private ObjectStatements objectStatements;

		public QueryStatement<String> loadProperties;
		public QueryStatement<String> findProperty;
		public UpdateStatement<Void> updateProperty;
		public UpdateStatement<Void> insertProperty;

		@PostConstruct
		private void init() {
			loadProperties = objectStatements.getQuery(String.class, "select property_value, property_name  from application_properties where persistance_name=?");
			updateProperty = objectStatements.getUpdate(Void.class, "update application_properties set property_value=? where persistance_name=? and property_name=?");
			insertProperty = objectStatements.getUpdate(Void.class, "insert into application_properties(persistance_name, property_name, property_value) values (?, ?, ?)");
			findProperty = objectStatements.getQuery(String.class, "select property_value  from application_properties where persistance_name=? and property_name = ?");
		}

	}
}

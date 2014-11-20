package com.knockchat.utils.meta.thrift;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.knockchat.utils.meta.MetaProperty;
import com.knockchat.utils.meta.asm.AsmMetaClass;
import com.knockchat.utils.meta.asm.DefiningClassLoader;
import com.knockchat.utils.meta.getset.GetSetPropertySupport;

public class ThriftMetaClass<T extends TBase> extends AsmMetaClass {
	
	private static final Logger log = LoggerFactory.getLogger(ThriftMetaClass.class); 

	public ThriftMetaClass(Class<T> objectClass, DefiningClassLoader classLoader ) {
		super(objectClass, classLoader);
		
		Map<? extends TFieldIdEnum, FieldMetaData> map = null;
		Class thriftClass = objectClass;
		do{
			thriftClass = thriftClass.getSuperclass();
			map = FieldMetaData.getStructMetaDataMap(thriftClass);			 
		}while(map == null);
				 
		fieldProperties.clear();
		
		for (Entry<? extends TFieldIdEnum, FieldMetaData> e: map.entrySet()){
			final MetaProperty bp = beanProperties.get(e.getKey().getFieldName());
			fieldProperties.put(e.getKey().getFieldName(), new ThriftMetaProperty(e.getKey(), e.getValue().valueMetaData, bp !=null ? bp.getType() : null));
		}

		beanProperties.clear();
		methods.clear();
		
		try {
	
			for ( Field field : objectClass.getFields() ) {
				
				if (field.getDeclaringClass().equals(thriftClass))
					continue;
								
				if ( ( field.getModifiers() & Modifier.STATIC) != 0 )
					continue;

				if ( ( field.getModifiers() & Modifier.FINAL) != 0 )
					continue;

				fieldProperties.put( field.getName(), buildMetaField( field ) );
			}
	
			for ( Method method : objectClass.getMethods() ) {
				
				if (method.getDeclaringClass().equals(thriftClass))
					continue;
				
				if ( ( method.getModifiers() & Modifier.STATIC) != 0 )
					continue;
				
				methods.put( method.getName(), buildMetaMethod( method ) );
			}
		} catch ( Throwable e ) {
			throw new Error("Can't create AsmMetaClass for class " + objectClass.getName(), e );
		}
		
		GetSetPropertySupport.get( this.getMethods(), beanProperties );		 		  						
	}

	@Override
	public MetaProperty getProperty( String propertyName ) {
		final MetaProperty t = getFieldProperty( propertyName );
		if (t!=null && t instanceof ThriftMetaProperty)
			return t;
		
		return super.getProperty(propertyName);
	}
	
}

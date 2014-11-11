package com.knockchat.utils.meta.thrift;

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

public class ThriftMetaClass<T extends TBase> extends AsmMetaClass {
	
	private static final Logger log = LoggerFactory.getLogger(ThriftMetaClass.class); 

	public ThriftMetaClass(Class<T> objectClass, DefiningClassLoader classLoader ) {
		super(objectClass, classLoader);
		
		Map<? extends TFieldIdEnum, FieldMetaData> map = null;
		Class thriftClass = objectClass;
		while(map == null){
			map = FieldMetaData.getStructMetaDataMap(thriftClass);
			thriftClass = thriftClass.getSuperclass(); 
		}
				
		for (Entry<? extends TFieldIdEnum, FieldMetaData> e: map.entrySet()){
			final MetaProperty bp = beanProperties.get(e.getKey().getFieldName());
			fieldProperties.put(e.getKey().getFieldName(), new ThriftMetaProperty(e.getKey(), e.getValue().valueMetaData, bp !=null ? bp.getType() : null));
		}
	}

	@Override
	public MetaProperty getProperty( String propertyName ) {
		final MetaProperty t = getFieldProperty( propertyName );
		if (t!=null && t instanceof ThriftMetaProperty)
			return t;
		
		return super.getProperty(propertyName);
	}
	
}

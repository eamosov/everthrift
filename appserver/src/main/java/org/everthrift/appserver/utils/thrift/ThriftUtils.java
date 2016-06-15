package org.everthrift.appserver.utils.thrift;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.everthrift.appserver.utils.Pair;

import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;

public class ThriftUtils {
	
	private static final Object NULL_RESULT = new Object();
	private static final AtomicReference<Reference2ObjectMap<Class, Object>> classes = new AtomicReference<Reference2ObjectMap<Class, Object>>(new Reference2ObjectOpenHashMap<Class, Object>());	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Pair<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>> getRootThriftClass(Class<? extends TBase> cls){
	
		Reference2ObjectMap<Class, Object> _classes = classes.get();		
		Object p = _classes.get(cls);
				
		if (p == null){
			p = _getRootThriftClass(cls);
			if (p == null)
				p = NULL_RESULT;
			
			_classes = new Reference2ObjectOpenHashMap<Class, Object>(_classes);
			_classes.put(cls, p);
			classes.set(_classes);
		}
		
		if (p == NULL_RESULT)
			return null;

		return (Pair<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>>)p;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static Pair<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>> _getRootThriftClass(Class<? extends TBase> cls){
		Map<? extends TFieldIdEnum, FieldMetaData> map = null;
		Class<? extends TBase> nextThriftClass = cls;
		Class<? extends TBase> thriftClass;
		do{
			thriftClass = nextThriftClass;
			try{
				map = FieldMetaData.getStructMetaDataMap(thriftClass);
			}catch(Exception e){
				map = null;
			}
			nextThriftClass = (Class<? extends TBase>)thriftClass.getSuperclass();
		}while(map == null && nextThriftClass !=null);
		
		if (map == null)
			return null;
		
		return Pair.<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>>create(thriftClass, map);
	}	
}

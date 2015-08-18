package com.knockchat.utils.thrift;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.RandomAccess;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class ThriftTraversal {
	
	private static final Logger log = LoggerFactory.getLogger(ThriftTraversal.class);
	
	private static Map<Class<? extends TBase>, ThriftTraversal> nodes = Maps.newIdentityHashMap(); 
	
	private final Class<? extends TBase> cls;	
	private final Map<TFieldIdEnum, FieldMetaData> fields;
	private final Map<Class<? extends TBase>, List<TFieldIdEnum>> routing = Maps.newIdentityHashMap();
		
	private ThriftTraversal(Class<? extends TBase> cls) {
		this.fields = (Map)FieldMetaData.getStructMetaDataMap(cls);
		this.cls = cls;
		
		if (fields == null)
			throw new RuntimeException("invalid argument");
	}
		
	private static  ThriftTraversal getNode(Class<? extends TBase> cls){
		synchronized(nodes){
			ThriftTraversal n = nodes.get(cls);
			if (n == null){
				n = new ThriftTraversal(cls);
				nodes.put(cls, n);
			}
			return n;			
		}
	}
	
	public static <T extends TBase> void visitChildsOfType(final Object obj, final Class<T> type, final Function<T, Void> visitHandler){
		visitChildsOfType(obj, type, Utils.getRootThriftClass(type).first, visitHandler);
	}
			
	private static <T extends TBase> void visitChildsOfType(final Object obj, final Class<T> type, final Class<? extends TBase> thriftBaseType, final Function<T, Void> visitHandler){
		
		if (log.isDebugEnabled())
			log.debug("visit: objClass={}, type={},  thriftBaseType={}, obj={}", obj==null ? null : obj.getClass(), type.getSimpleName(), thriftBaseType.getSimpleName(), obj);
		
		if (obj == null)
			return;
		
		if (type.isInstance(obj)){
			visitHandler.apply((T)obj);
			return;
		}
		
		
		if (obj instanceof TBase){
			
			final ThriftTraversal node = getNode(Utils.getRootThriftClass((Class)obj.getClass()).first);
			
			final List<TFieldIdEnum> _l = node.getChildsOfType(thriftBaseType);
			
			for (int i=0; i< _l.size(); i++){
				final TFieldIdEnum f = _l.get(i);
			
				if (log.isDebugEnabled())
					log.debug("visit field {} of class {}", f, obj.getClass());
				
				visitChildsOfType(((TBase)obj).getFieldValue(f), type, thriftBaseType, visitHandler);
			}
			
			return;
			
		}else if (obj instanceof Collection){
			
			if (((Collection)obj).isEmpty())
				return;
			
			if (obj instanceof RandomAccess){
				final List _l = (List)obj;
				for (int i=0; i<_l.size(); i++)
					visitChildsOfType(_l.get(i), type, thriftBaseType, visitHandler);
			}else{
				for (Object i: ((Collection)obj))
					visitChildsOfType(i, type, thriftBaseType, visitHandler);				
			}
			
			return;				
		}else if (obj instanceof Map){
			if (((Map)obj).isEmpty())
				return;
			
			for (Map.Entry e: ((Map<?,?>)obj).entrySet()){
				visitChildsOfType(e.getKey(), type, thriftBaseType, visitHandler);
				visitChildsOfType(e.getValue(), type, thriftBaseType, visitHandler);
			}

			return;
		}else{
			return;
		}		
	}
	
	private static boolean hasChildsOfType(FieldValueMetaData valueMetaData, Class type){

		if (valueMetaData instanceof StructMetaData){
			
			if ( ((StructMetaData)valueMetaData).structClass == type)
				return true;
			
			final Map<TFieldIdEnum, FieldMetaData> map = (Map)FieldMetaData.getStructMetaDataMap(((StructMetaData)valueMetaData).structClass);
			for (Entry<TFieldIdEnum, FieldMetaData> e :map.entrySet()){
				if (hasChildsOfType(e.getValue().valueMetaData, type))
					return true;
			}
			
			return false;						
		}else if (valueMetaData instanceof ListMetaData){
			return hasChildsOfType(((ListMetaData)valueMetaData).elemMetaData, type); 
		}else if (valueMetaData instanceof SetMetaData){
			return hasChildsOfType(((SetMetaData)valueMetaData).elemMetaData, type);
		}else if (valueMetaData instanceof MapMetaData){
			return hasChildsOfType(((MapMetaData)valueMetaData).valueMetaData, type) || hasChildsOfType(((MapMetaData)valueMetaData).keyMetaData, type);
		}else{
			return false;
		}
	}
	
	private synchronized List<TFieldIdEnum> getChildsOfType(Class type){
		
				
		List<TFieldIdEnum> childFields = routing.get(type);
		if (childFields == null){
			childFields = Lists.newArrayList();
			for (Entry<TFieldIdEnum, FieldMetaData> e: fields.entrySet()){
				if (hasChildsOfType(e.getValue().valueMetaData, type)){
					childFields.add(e.getKey());
				}					
			}
			log.debug("root class={}, type={}, fields={}", cls, type, childFields);
			routing.put(type, childFields);
		}
		
		return childFields;		
	}

}

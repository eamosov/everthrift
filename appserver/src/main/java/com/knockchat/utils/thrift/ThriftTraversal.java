package com.knockchat.utils.thrift;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.RandomAccess;
import java.util.Set;

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

import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ReferenceOpenHashSet;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class ThriftTraversal {
	
	private static final Logger log = LoggerFactory.getLogger(ThriftTraversal.class);
	
	private static Reference2ObjectMap<Class<? extends TBase>, ThriftTraversal> nodes = new Reference2ObjectOpenHashMap<Class<? extends TBase>, ThriftTraversal>(); 
	
	private final Class<? extends TBase> cls;	
	private final Map<TFieldIdEnum, FieldMetaData> fields;
	private final Reference2ObjectMap<Class<? extends TBase>, List<TFieldIdEnum>> routing = new Reference2ObjectOpenHashMap<Class<? extends TBase>, List<TFieldIdEnum>>();
		
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
	
	public static <T extends TBase> Set<T> visitChildsOfType(final Object obj, final Class<T> type, final Function<T, Void> visitHandler){
		final Set<T> visited = new ReferenceOpenHashSet<T>();
		visitChildsOfType(visited, obj, type, Utils.getRootThriftClass(type).first, visitHandler);
		return visited;
	}
			
	private static <T extends TBase> void visitChildsOfType(Set<T> visited, final Object obj, final Class<T> type, final Class<? extends TBase> thriftBaseType, final Function<T, Void> visitHandler){
		
		if (obj == null)
			return;

		if (log.isDebugEnabled()){
			String id=null;
			try {
				Method m = obj.getClass().getMethod("getId");
				id = m.invoke(obj).toString();
			} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				id = "<unknown>";
			}
			log.debug("visit: objClass={}, type={},  thriftBaseType={}, id={}", obj==null ? null : obj.getClass().getSimpleName(), type.getSimpleName(), thriftBaseType.getSimpleName(), id);
		}
				
		if (type.isInstance(obj)){
			if (visited.add((T)obj)){
				visitHandler.apply((T)obj);
			}else{
				log.debug("allready visited: objClass={}, type={},  thriftBaseType={}",  obj==null ? null : obj.getClass().getSimpleName(), type.getSimpleName(), thriftBaseType.getSimpleName());
			}
			return;
		}		
		
		if (obj instanceof TBase){
			
			final ThriftTraversal node = getNode(Utils.getRootThriftClass((Class)obj.getClass()).first);
			
			final List<TFieldIdEnum> _l = node.getChildsOfType(thriftBaseType);
			
			for (int i=0; i< _l.size(); i++){
				final TFieldIdEnum f = _l.get(i);
			
				if (log.isDebugEnabled())
					log.debug("visit field {} of class {}", f, obj.getClass());
				
				visitChildsOfType(visited, ((TBase)obj).getFieldValue(f), type, thriftBaseType, visitHandler);
			}
			
			return;
			
		}else if (obj instanceof Collection){
			
			if (((Collection)obj).isEmpty())
				return;
			
			if (obj instanceof RandomAccess){
				final List _l = (List)obj;
				for (int i=0; i<_l.size(); i++)
					visitChildsOfType(visited, _l.get(i), type, thriftBaseType, visitHandler);
			}else{
				for (Object i: ((Collection)obj))
					visitChildsOfType(visited, i, type, thriftBaseType, visitHandler);				
			}
			
			return;				
		}else if (obj instanceof Map){
			if (((Map)obj).isEmpty())
				return;
			
			for (Map.Entry e: ((Map<?,?>)obj).entrySet()){
				visitChildsOfType(visited, e.getKey(), type, thriftBaseType, visitHandler);
				visitChildsOfType(visited, e.getValue(), type, thriftBaseType, visitHandler);
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

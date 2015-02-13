package com.knockchat.utils.thrift;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.collections.CollectionUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.knockchat.utils.Pair;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class ThriftTraversal {
	
	enum EDGE_TYPE{
		OBJECT,
		COLLECTION,
		MAP_VALUES
	}
	
	private static Map<Class<? extends TBase>, ThriftTraversal> nodes = Maps.newIdentityHashMap(); 
	
	private final Class<? extends TBase> cls;
	private Map<TFieldIdEnum, Pair<EDGE_TYPE,ThriftTraversal>> edges = Maps.newHashMap();
	private Map<Class<? extends TBase>, List<TFieldIdEnum>> routing = Maps.newIdentityHashMap();
	private boolean scanned;
	
	private static final ReadWriteLock lock = new ReentrantReadWriteLock();
	
	private ThriftTraversal(Class<? extends TBase> cls) {
		this.cls = cls;				
	}
	
	private ThriftTraversal addEdge(TFieldIdEnum id, EDGE_TYPE type, Class<? extends TBase> cls){
		final ThriftTraversal t = getNode(cls);
		edges.put(id, Pair.create(type, t));
		return t;
	}
	
	private static  ThriftTraversal getNode(Class<? extends TBase> cls){
		ThriftTraversal n = nodes.get(cls);
		if (n == null){
			n = new ThriftTraversal(cls);
			nodes.put(cls, n);
		}
		return n;
	}
		
	public static <T extends TBase> void visitChildsOfType(final Object obj, final Class<T> type, Function<T, Void> visitHandler){
		
		if (obj == null)
			return;
		
		final EDGE_TYPE edgeType;
		final Class<TBase> objClass;
		
		if (obj instanceof TBase){
			objClass = (Class<TBase>)obj.getClass();
			edgeType = EDGE_TYPE.OBJECT;
		}else if (obj instanceof Collection){
			if (((Collection)obj).isEmpty())
				return;
				
			edgeType = EDGE_TYPE.COLLECTION;
			objClass = (Class<TBase>)((Collection)obj).iterator().next().getClass();
		}else if (obj instanceof Map){
			if (((Map)obj).isEmpty())
				return;

			edgeType = EDGE_TYPE.MAP_VALUES;
			objClass = (Class<TBase>)((Map)obj).values().iterator().next().getClass();
		}else{
			return;
		}
		
		final Pair<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>> root = Utils.getRootThriftClass(objClass);

		
		final ThriftTraversal node;
		lock.writeLock().lock();
		try{
			node = scanOneClass(root.first, root.second);			
			node.analizeChildsOfType(type);			
		}finally{
			lock.writeLock().unlock();
		}		
		
		lock.readLock().lock();
		try{			
			node.visitChildsOfType(edgeType, obj, type, visitHandler);
		}finally{			
			lock.readLock().unlock();
		}
	}
	
	private <T extends TBase> void invokeHandler(EDGE_TYPE objType, Object obj, Class<T> type, Function<T, Void> visitHandler){
		
		switch(objType){
		case OBJECT:
			if (type.isInstance(obj))
				visitHandler.apply((T)obj);
			break;
		case COLLECTION:
			for (Object i : ((Collection)obj))
				if (type.isInstance(i))
					visitHandler.apply((T)i);
			break;
		case MAP_VALUES:
			for (Object i : ((Map)obj).values())
				if (type.isInstance(i))
					visitHandler.apply((T)i);
			break;			
		}
	}
			
	private <T extends TBase> void visitChildsOfType(EDGE_TYPE objType, Object obj, Class<T> type, Function<T, Void> visitHandler){
		
		if (obj == null)
			return;
						
		if (cls.isAssignableFrom(type)){
			invokeHandler(objType, obj, type, (Function)visitHandler);
			return;
		}

		final List<TFieldIdEnum> typeRouting = this.routing.get(type);
		if (CollectionUtils.isEmpty(typeRouting))
			return;
		
		if (objType == EDGE_TYPE.COLLECTION){
			
			for (Object i : (Collection)obj)
				visitChildsOfType(EDGE_TYPE.OBJECT, i, type, visitHandler);

			return;
		}else if (objType == EDGE_TYPE.MAP_VALUES){
			
			for (Object i : ((Map)obj).values())
				visitChildsOfType(EDGE_TYPE.OBJECT, i, type, visitHandler);

			return;			
		}
		
		for (TFieldIdEnum id: typeRouting){			
			final Pair<EDGE_TYPE, ThriftTraversal> v = edges.get(id);
			final EDGE_TYPE edgeType = v.first;
			final ThriftTraversal childNode = v.second;

			final Object fieldValue =  ((TBase)obj).getFieldValue(id);
			if (fieldValue !=null){
				childNode.visitChildsOfType(edgeType, fieldValue, type, visitHandler);
			}
		}
						
		return;
	}

	private boolean analizeChildsOfType(Class type){
		
		if (cls.isAssignableFrom(type))
			return true;
		
		List<TFieldIdEnum> r = routing.get(type);
		
		if (r!=null)
			return !r.isEmpty();
						
		r = Lists.newArrayList();
				
		for (Entry<TFieldIdEnum, Pair<EDGE_TYPE, ThriftTraversal>> e: edges.entrySet()){
			final TFieldIdEnum id = e.getKey();
			final ThriftTraversal childNode = e.getValue().second;
			
			final boolean tmp  = childNode.analizeChildsOfType(type);
			if (tmp)
				r.add(id);
			
		}
		
		routing.put(type, r);		
		return !r.isEmpty();		
	}

	private boolean isScanned() {
		return scanned;
	}

	private void setScanned(boolean scanned) {
		this.scanned = scanned;
	}
	
	private static ThriftTraversal scanOneClass(final Class<? extends TBase> cls, Map<? extends TFieldIdEnum, FieldMetaData> map){
		
		ThriftTraversal node = getNode(cls);
		if (node.isScanned())
			return node;
						
		node.scanOneClass(map);
		return node;
	}
	
	private void scanOneClass(Map<? extends TFieldIdEnum, FieldMetaData> structMap){
		
		setScanned(true);
		
		for (Entry<? extends TFieldIdEnum, FieldMetaData> e: structMap.entrySet()){
			for (Class<? extends TBase> cls :scanOneField(e.getKey(), e.getValue())){
				scanOneClass(cls, FieldMetaData.getStructMetaDataMap(cls));
			}
		}		
	}
	
	private List<Class<? extends TBase>> scanOneField(TFieldIdEnum id, FieldMetaData fmd){
		
		final List<Class<? extends TBase>> ret = Lists.newArrayList();
					
		if (fmd.valueMetaData instanceof StructMetaData){
			ret.add(addEdge(id, EDGE_TYPE.OBJECT, ((StructMetaData)fmd.valueMetaData).structClass).cls);
		}else if (fmd.valueMetaData instanceof ListMetaData){
			
			final FieldValueMetaData elemMetaData = ((ListMetaData)fmd.valueMetaData).elemMetaData;
			
			if (elemMetaData instanceof StructMetaData)					
				ret.add(addEdge(id, EDGE_TYPE.COLLECTION, ((StructMetaData) elemMetaData).structClass).cls);
			
		}else if (fmd.valueMetaData instanceof SetMetaData){
			final FieldValueMetaData elemMetaData = ((SetMetaData)fmd.valueMetaData).elemMetaData;
			
			if (elemMetaData instanceof StructMetaData)					
				ret.add(addEdge(id, EDGE_TYPE.COLLECTION, ((StructMetaData) elemMetaData).structClass).cls);
			
		}else if (fmd.valueMetaData instanceof MapMetaData){
			final FieldValueMetaData valueMetaData = ((MapMetaData)fmd.valueMetaData).valueMetaData;
						
			if (valueMetaData instanceof StructMetaData)
				ret.add(addEdge(id, EDGE_TYPE.MAP_VALUES, ((StructMetaData) valueMetaData).structClass).cls);
		}
		
		return ret;
	}

}

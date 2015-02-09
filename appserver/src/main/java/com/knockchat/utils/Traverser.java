package com.knockchat.utils;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class Traverser {

	private static class Adapter{
		final Class<? extends TBase> rootClass;	
		final Class traverseClass;
		final TFieldIdEnum field;
		
		public Adapter(Class<? extends TBase> rootClass, Class traverseClass, TFieldIdEnum field) {
			super();
			this.rootClass = rootClass;
			this.traverseClass = traverseClass;
			this.field = field;
		}		
	}
	
	private final List<Adapter> adapters = Lists.newArrayList();

	public Traverser() {
		
	}
	
	public void addTBaseMapping(Class<? extends TBase> rootClass, Class traverseClass, TFieldIdEnum field){
		adapters.add(new Adapter(rootClass, traverseClass, field));
	}

	public <T> void traverse(Class<T> traverseClass, Function<T, Void> f, Object root){
		
		if (root == null)
			return;
		
		if (traverseClass.isInstance(root)){
			f.apply((T)root);
		}else if (root instanceof Iterable){
			for (Object i: (Iterable)root)
				traverse(traverseClass, f, i);
		}else if (root instanceof Map){
			final Set<Entry> s =  ((Map)root).entrySet();
			for (Entry e: s){
				traverse(traverseClass, f, e.getKey());
				traverse(traverseClass, f, e.getValue());
			}
		}else if (root instanceof TBase){			
			for (Adapter a: adapters){
				if (a.rootClass.isInstance(root) && traverseClass.isAssignableFrom(a.traverseClass)){
					traverse(traverseClass, f, ((TBase)root).getFieldValue(a.field));
					break;
				}
			}
		}
	}
	
}

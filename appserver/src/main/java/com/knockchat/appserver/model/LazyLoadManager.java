package com.knockchat.appserver.model;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class LazyLoadManager {
	
	private static final Logger log = LoggerFactory.getLogger(LazyLoadManager.class);
	
	public static int MAX_LOAD_ITERATIONS = 5;
	
	public static class LoadList{
		final Multimap<Function<Iterable, Integer>, Object> loadList = HashMultimap.create();
		boolean buildLoadList;
		public boolean enabled=true;
		
		public Function<Object, Void> walker = new Function<Object, Void>(){

			@Override
			public Void apply(Object input) {
				invokeLoadAll(input);
				return null;
			}
			
		};
		
		public void reset(){
			loadList.clear();
			buildLoadList = false;
		}
		
		private int doLoad(){
			buildLoadList = false;
			int nLoaded = 0;
			for (Map.Entry<Function<Iterable, Integer>, Collection<Object>> e : loadList.asMap().entrySet()){
				if (!e.getValue().isEmpty()){
					final Integer n = e.getKey().apply(e.getValue());
					if (n!=null)
						nLoaded += n;
				}
			}
			return nLoaded;
			//loadList.clear();
		}
		
		public void start(){
			loadList.clear();
			buildLoadList = true;
		}
		
		public void enable(){
			enabled = true;
		}
		
		public void disable(){
			enabled = false;
		}		
		
		public int load(Object o, Function<Object, Void> walker){
			return load(o, MAX_LOAD_ITERATIONS, walker);
		}

		public int load(Object o){
			return load(o, MAX_LOAD_ITERATIONS, walker);
		}
		
		public int load(Object o, int maxIterations){
			return load(o, maxIterations, walker);
		}		
		
		private void invokeLoadAll(Object o){
			try {
				o.getClass().getMethod("loadAll").invoke(o);
			} catch (IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException
					| SecurityException e) {
				log.error("Error while invoking loadAll():", e);
			}						
		}
		
		public int load(Object o, int maxIterations, Function<Object, Void> walker){
						
			if (!enabled)
				return 0;
			
			int nAllLoaded=0;
			int lap=0;			
			int nLoaded;
			
			do{
				nLoaded =0;
				start();
				
				log.debug("Starting load iteration: {}", lap);
				final long st = System.nanoTime();
				
				try{
					walker.apply(o);					
					nLoaded = doLoad();
				}finally{
					reset();
				}
				
				final long end = System.nanoTime();
				if (log.isDebugEnabled())
					log.debug("Iteration {} finished. {} entities loaded with {} mcs", new Object[]{lap, nLoaded, (end -st)/1000});
				
				lap++;
				nAllLoaded += nLoaded;
			}while(nLoaded > 0 && lap < maxIterations);
			
			return nAllLoaded;
		}
		
	}
	
	private static final ThreadLocal <LoadList> loadList =  new ThreadLocal <LoadList> (){ 
			@Override
			protected LoadList initialValue() {
				return new LoadList();
			}
	};

	public static LoadList get(){
		return loadList.get();
	}
	
	public static boolean disable(){
		final LoadList l = loadList.get();
		final boolean old = l.enabled; 
		l.reset();
		l.disable();
		return old;
	}

	public static boolean enable(){
		final LoadList l = loadList.get();
		final boolean old = l.enabled;
		l.enable();
		return old;
	}
	
	public static boolean setEnabled(boolean value){
		final LoadList l = loadList.get();
		final boolean old = l.enabled; 

		if (value){
			l.enable();
		}else{
			l.reset();
			l.disable();
		}
		
		return old;
	}
	
	public static boolean addToLoad(Function<Iterable, Integer> function, Object object){
		final LoadList l = loadList.get();
		if (l.buildLoadList){
			l.loadList.put(function, object);
			return true;
		}
		return false;
	}
	
	public static int load(Object o){
		return get().load(o);
	}
	
	public static int load(Object o, int maxIterations){
		return get().load(o, maxIterations);
	}

	public static void load(int maxIterations, final Runnable walker){
		boolean lazyLoaderStatus = LazyLoadManager.enable();
		final LoadList ll = LazyLoadManager.get();
		ll.load(null, maxIterations, new Function<Object, Void>(){

			@Override
			public Void apply(Object input) {
				walker.run();
				return null;
			}});		
		LazyLoadManager.setEnabled(lazyLoaderStatus);		
	}
}

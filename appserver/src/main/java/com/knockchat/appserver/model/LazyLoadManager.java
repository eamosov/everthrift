package com.knockchat.appserver.model;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LazyLoadManager {
	
	private static final Logger log = LoggerFactory.getLogger(LazyLoadManager.class);
	
	public static int MAX_LOAD_ITERATIONS = 5;
	
	public static String SCENARIO_DEFAULT = "default";
	public static String SCENARIO_ADMIN = "admin";
	public static String SCENARIO_JSON = "json";
	
	
	public static String LOAD_ALL = "loadAll";
	public static String LOAD_JSON = "loadJson";
	public static String LOAD_ADMIN = "loadAdmin";
			
	public static int load(int maxIterations, Object o, Registry r, WalkerIF walker){
		
		log.debug("load, maxIterations={}, o.class={}", maxIterations, o.getClass().getSimpleName());
					
		
		int nAllLoaded=0;
		int lap=0;			
		int nLoaded;
		
		
		do{
			nLoaded =0;
			
			log.debug("Starting load iteration: {}", lap);
			final long st = System.nanoTime();
			
			walker.apply(o);					
			nLoaded = r.load();
			
			final long end = System.nanoTime();
			if (log.isDebugEnabled())
				log.debug("Iteration {} finished. {} entities loaded with {} mcs", new Object[]{lap, nLoaded, (end -st)/1000});
			
			lap++;
			nAllLoaded += nLoaded;
		}while(nLoaded > 0 && lap < maxIterations);
				
		return nAllLoaded;
	}
	
	public static <T> T load(final String scenario, final String[] methods, int maxIterations, final T o){
		
		if (o == null ||
			(o instanceof Collection && ((Collection)o).isEmpty()) ||
			(o instanceof Map && ((Map)o).isEmpty()))
			return o;
		
		final Registry r = new RegistryImpl();
		final WalkerIF walker = new RecursiveWalker(r, scenario, methods);
		load(maxIterations, o, r, walker);
		return o;
	}		

	public static <T> T load(final String scenario, final String[] methods, final T o){
		return load(scenario, methods, MAX_LOAD_ITERATIONS, o);
	}
	
	public static <T> T loadForJson(final T o){
		return load(LazyLoadManager.SCENARIO_JSON, new String[]{LazyLoadManager.LOAD_JSON, LazyLoadManager.LOAD_ALL}, o);
	}
	
}

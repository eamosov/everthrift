package com.knockchat.appserver.model;

import java.util.Collection;
import java.util.Map;

import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.knockchat.utils.ClassUtils;
import com.knockchat.utils.thrift.scanner.ScenarioAwareIF;
import com.knockchat.utils.thrift.scanner.TBaseScanHandler;
import com.knockchat.utils.thrift.scanner.TBaseScanner;
import com.knockchat.utils.thrift.scanner.TBaseScannerFactory;

public class LazyLoadManager {
	
	private static final Logger log = LoggerFactory.getLogger(LazyLoadManager.class);
	
	public static int MAX_LOAD_ITERATIONS = 5;
	
	public static String SCENARIO_DEFAULT = "default";
	public static String SCENARIO_ADMIN = "admin";
	public static String SCENARIO_JSON = "json";
	
	private static final String defaultFields[] = new String[]{"*"};
	
	public static String LOAD_ALL = "loadAll";
	public static String LOAD_JSON = "loadJson";
	public static String LOAD_ADMIN = "loadAdmin";
	
	private final static TBaseScannerFactory scannerFactory = new TBaseScannerFactory();
	
	public static interface WalkerIF{
		void apply(Object o);
	}
	
	public static class RecursiveWalker implements WalkerIF{
		
		private final String[] methods;
		private final String scenario;
		
		public RecursiveWalker(String scenario, String method){
			this.scenario = scenario;
			this.methods = new String[]{method};
		}

		public RecursiveWalker(String scenario, String ... methods){
			this.scenario = scenario;
			this.methods = methods;
		}

		@Override
		public void apply(Object o) {
			recursive(o);			
		}
		
		private String[] getScenario(Object o){
			
			if (!(o instanceof ScenarioAwareIF))
				return defaultFields;
		
			String s[] = ((ScenarioAwareIF)o).getScenario(scenario);
			
			if (s == null)
				s = ((ScenarioAwareIF)o).getScenario(SCENARIO_DEFAULT);
			
			if (s == null)
				s = defaultFields; 				
			
			return s;
		}
		
		private void invoke(Object o){

			
			if (o instanceof TBase){
				
				final TBaseScanHandler h = new TBaseScanHandler(){

					@Override
					public void apply(TBase o) {
						scannerFactory.create(o.getClass(), getScenario(o)).scan(o, this);
						ClassUtils.invokeFirstMethod(methods, o);					
					}			
				};
				
				scannerFactory.create((Class)o.getClass(), getScenario(o)).scan((TBase)o, h);				
			}
			
			ClassUtils.invokeFirstMethod(methods, o);
		}
				
		private void recursive(final Object o){
			
			if (o == null)
				return;

			if (o instanceof Iterable){
				for (Object i: ((Iterable)o))
					if (i!=null)
						invoke(i);
			}else if (o instanceof Map){
				for (Object i: ((Map)o).values()){
					if (i!=null)
						invoke(i);
				}
			}else{
				invoke(o);
			}
		}		
	}
	
	//private static final WalkerIF loadAllWalker = new RecursiveWalker("loadAll");
	//private static final WalkerIF loadExtraWalker = new RecursiveWalker("loadExtra", "loadAll");

	public static class LoadList{
		final Multimap<Function<Iterable, Integer>, Object> loadList = HashMultimap.create();
		boolean buildLoadList;
		public boolean enabled=true;
				
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
		
		public int load(Object o, WalkerIF walker){
			return load(o, MAX_LOAD_ITERATIONS, walker);
		}

		public int load(final String scenario, final String[] methods, final Object o){
			return load(o, MAX_LOAD_ITERATIONS, new RecursiveWalker(scenario, methods));
		}
		
		public int load(final String scenario, final String[] methods, int maxIterations, final Object o){
			return load(o, maxIterations, new RecursiveWalker(scenario, methods));
		}
				
		public int load(Object o, int maxIterations, WalkerIF walker){
						
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
	
	public static void load(final String scenario, final String[] methods, final Object o){
		load(scenario, methods, MAX_LOAD_ITERATIONS, o);
	}
	
	public static void loadForJson(final Object o){
		LazyLoadManager.load(LazyLoadManager.SCENARIO_JSON, new String[]{LazyLoadManager.LOAD_JSON, LazyLoadManager.LOAD_ALL}, LazyLoadManager.MAX_LOAD_ITERATIONS, o);
	}
	
	public static void load(final String scenario, final String[] methods, int maxIterations, final Object o){
		
		if (o == null)
			return;
		
		boolean lazyLoaderStatus = LazyLoadManager.enable();
		final LoadList ll = LazyLoadManager.get();
		ll.load(scenario, methods, maxIterations, o);		
		LazyLoadManager.setEnabled(lazyLoaderStatus);		
	}		
}

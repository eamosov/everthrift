package com.knockchat.appserver.model;

import java.util.List;
import java.util.Map;
import java.util.RandomAccess;

import org.apache.thrift.TBase;

import com.knockchat.utils.ClassUtils;
import com.knockchat.utils.thrift.scanner.ScenarioAwareIF;
import com.knockchat.utils.thrift.scanner.TBaseScanHandler;
import com.knockchat.utils.thrift.scanner.TBaseScannerFactory;

public class RecursiveWalker implements WalkerIF {
	
	public static String SCENARIO_DEFAULT = "default";
	
	
	private final static TBaseScannerFactory scannerFactory = new TBaseScannerFactory();
	private static final String defaultFields[] = new String[]{"*"};

	private final Registry registry;
	private final String[] methods;
	private final String scenario;
	
	private final TBaseScanHandler tBaseScanHandler = new TBaseScanHandler(){

		@Override
		public void apply(TBase o) {				
			scannerFactory.create(o.getClass(), getScenario(o)).scan(o, this, registry);
			ClassUtils.invokeFirstMethod(methods, o, registry);					
		}			
	};
	
	public RecursiveWalker(Registry registry, String scenario, String method){
		this.registry = registry;
		this.scenario = scenario;
		this.methods = new String[]{method};
	}

	public RecursiveWalker(Registry registry, String scenario, String ... methods){
		this.registry = registry;
		this.scenario = scenario;
		this.methods = methods;
	}

	@Override
	public void apply(Object o) {
		registry.clear();
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
		
	private void recursive(final Object o){
					
		if (o == null)
			return;

		if (o instanceof RandomAccess){
			final List _l = (List)o;
			for (int i=0; i<_l.size(); i++){
				final Object j = _l.get(i);
				if (j!=null)
					recursive(j);
			}
				
		}else if (o instanceof Iterable){
			for (Object i: ((Iterable)o))
				if (i!=null)
					recursive(i);
		}else if (o instanceof Map){
			for (Object i: ((Map)o).values()){
				if (i!=null)
					recursive(i);
			}
		}else{
			
			if (o instanceof TBase){				
				scannerFactory.create((Class)o.getClass(), getScenario(o)).scan((TBase)o, tBaseScanHandler, registry);				
			}
			
			ClassUtils.invokeFirstMethod(methods, o, registry);
		}
	}	
}

package com.knockchat.appserver.model.lazy;

import java.util.List;
import java.util.Map;
import java.util.RandomAccess;

import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.knockchat.utils.thrift.scanner.TBaseScanHandler;
import com.knockchat.utils.thrift.scanner.TBaseScanner;
import com.knockchat.utils.thrift.scanner.TBaseScannerFactory;

public class RecursiveWalker implements WalkerIF {
	
	private static final Logger log = LoggerFactory.getLogger(RecursiveWalker.class);
	
	public static String SCENARIO_DEFAULT = "default";
	
	
	private final static TBaseScannerFactory scannerFactory = new TBaseScannerFactory();
	private static final String defaultFields[] = new String[]{"*"};

	private final Registry registry;
	private final String scenario;
	
	private final TBaseScanHandler tBaseScanHandler = new TBaseScanHandler(){

		@Override
		public void apply(TBase o) {
			
			if (o == null)
				return;
			
			final TBaseScanner s = scannerFactory.create(o.getClass(), scenario);
			
			if (s == null){
				log.error("Coudn't get TBaseScanner for class={} and scenario={}", o.getClass().getSimpleName(), scenario);
				return;
			}
			
			s.scan(o, this, registry);
		}			
	};
	
	public RecursiveWalker(Registry registry, String scenario){
		this.registry = registry;
		this.scenario = scenario;
	}

	@Override
	public void apply(Object o) {
		registry.clear();
		recursive(o);			
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
				scannerFactory.create((Class)o.getClass(), scenario).scan((TBase)o, tBaseScanHandler, registry);				
			}
			
		}
	}	
}

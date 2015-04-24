package com.knockchat.utils.thrift.scanner;

import java.util.Map;

import com.google.common.collect.Maps;
import com.knockchat.appserver.model.LazyLoadManager;

public class Scenarios {
	
	public static class Builder{
		private final Map<String, String[]> s = Maps.newHashMap();
		
		private Builder(){
			
		}
		
		public static Builder create(){
			return new Builder();
		}
		
		public Builder defaults(String ... fields){
			return add(LazyLoadManager.SCENARIO_DEFAULT, fields);
		}

		public Builder admin(String ... fields){
			return add(LazyLoadManager.SCENARIO_ADMIN, fields);
		}

		public Builder json(String ... fields){
			return add(LazyLoadManager.SCENARIO_JSON, fields);
		}
		
		public Builder add(String scenarion, String ... fields){
			s.put(scenarion, fields);
			return this;
		}
		
		public Scenarios build(){
			return new Scenarios(s); 
		}
	}
	
	private final Map<String, String[]> s;

	private Scenarios(Map<String, String[]> s) {
		this.s = Maps.newHashMap(s);
	}
	
	public String[] get(String name){
		return s.get(name);
	}

}

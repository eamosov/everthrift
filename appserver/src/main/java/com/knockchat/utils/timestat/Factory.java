package com.knockchat.utils.timestat;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class Factory<T extends StatIF> {
	public static final Logger log = LoggerFactory.getLogger(Factory.class);

	private ConcurrentHashMap<String, T> stats = new ConcurrentHashMap<String, T>();
	Class<T> cls;
	
	public Factory(Class<T> aCls){
		cls = aCls;
	}
	
	public T createStatObject(String desc) throws ExistEx {
		T s;

		if (stats.get(desc) != null)
			throw new ExistEx("element '" + desc + "' allready exist");

		try {
			s = cls.newInstance();

		} catch (InstantiationException e) {
			log.error("InstantiationException", e);
			return null;
		} catch (IllegalAccessException e) {
			log.error("IllegalAccessException", e);
			return null;
		}

		s.setDesc(desc);

		if (stats.putIfAbsent(desc, s) !=null)
			throw new ExistEx("element '" + desc + "' allready exist");
			
		return s;
	}
	
	public boolean containsKey(String desc){
		return stats.containsKey(desc);
	}
	
	public T getOrCreate(String desc){
		T t = null;
		
		while(t==null){
			if ((t = stats.get(desc))==null){
				try {
					t = createStatObject(desc);
				} catch (ExistEx e) {
					t = null;
				}
			}			
		}
		return t;
	}
		
	public String getStats(boolean doReset){
		String ret = "";
		for( StatIF i: stats.values()){
			ret += i.getStats(doReset) + "\n";
		}
		return ret;
	}

	public String getStats(){
		String ret = "";
		for( StatIF i: stats.values()){
			ret += i.getStats() + "\n";
		}
		return ret;
	}
	
	public String getStats(String desc, boolean doReset){
		StatIF i=stats.get(desc);
			
		if (i!=null)
				return i.getStats(doReset);
		return "";
	}

}

package com.knockchat.utils.timestat;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class StatFactory {
	
	static Factory<TimeStat> timeStats = new Factory<TimeStat>(TimeStat.class);
	static Factory<Counter> counterStats = new Factory<Counter>(Counter.class);
	static Factory<Value> valueStats = new Factory<Value>(Value.class);
	
	public static TimeStat createTimeStat(String desc) throws ExistEx{
		return timeStats.createStatObject(desc);		
	}
	
	public static boolean hasTimeStats(String desc){
		return timeStats.containsKey(desc);
	}
	
	public static TimeStat getOrCreateTimeStat(String desc){
		return timeStats.getOrCreate(desc);
	}
	
	public static Counter createCounter(String desc) throws ExistEx{
		return counterStats.createStatObject(desc);
	}
	
	public static Value createValue(String desc) throws ExistEx{
		return valueStats.createStatObject(desc);
	}
		
	public static String getStats(boolean doReset){
		String ret = "<STATS>\n";
	
		ret += timeStats.getStats(doReset);
		ret += counterStats.getStats(doReset);
		ret += valueStats.getStats(doReset);
		ret += "</STATS>";
		return ret;
	}

	public static String getStats(){
		String ret = "<STATS>\n";
	
		ret += timeStats.getStats();
		ret += counterStats.getStats();
		ret += valueStats.getStats();
		ret += "</STATS>";
		return ret;
	}
	
	public static String getStats(String desc, boolean doReset){
		String ret = "<STATS>\n";
		
		ret += timeStats.getStats(desc, doReset);
		ret += counterStats.getStats(desc, doReset);
		ret += valueStats.getStats(desc, doReset);
		ret += "</STATS>";
		return ret;
	}

}

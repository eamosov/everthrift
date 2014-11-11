package com.knockchat.utils.timestat;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public abstract class StatIF {
	private String desc;
	boolean autoReset=true;
	
	long min;
	long max;
	long avr;
	long sum;
	long count;
	
	StatIF(){
		reset();
	}
	
	public void reset(){
		synchronized (this){
			count = 0;
			sum = 0;
			avr = 0;
			min = Long.MAX_VALUE;
			max = 0;
		}
	}
	
	void addValue(long val){

		synchronized (this){
			if (val > max)
				max = val;
		
			if (val < min)
				min = val;
		
			count ++;
			sum += val;
			avr = sum / count;
		}
	}
	
	String setDesc(String aDesc){
		String old = desc;
		desc = aDesc;
		return old;
	}
	
	String getDesc(){
		return desc;
	}
	
	public boolean setAutoReset(boolean aAutoReset){
		boolean old = autoReset;
		autoReset = aAutoReset;
		return old;
	}

	abstract String getStatsInternal();

	public String getStats(boolean doReset){
		String ret = getStatsInternal();
		if (doReset) reset();
		return ret;
	}
	
	public String getStats(){
		return getStats(autoReset);
	}
	
	@Override
	public String toString(){
		return getStatsInternal();
	}

}

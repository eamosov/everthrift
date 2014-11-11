package com.knockchat.utils.timestat;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class Counter extends StatIF {
	
	@Override
	public void reset(){
		count = 0;
	}
	
	public Counter(String aDesc){
		setDesc(aDesc);
		reset();
	}

	public Counter(){
		setDesc("Counter");
		reset();
	}
	
	public void hit(){
		count ++;
	}
		
	@Override
	String getStatsInternal(){
		String ret="<COUNTER desc=\"" + getDesc() + "\" count=\"" + count + "\" reset=\"" + autoReset + "\"/>";
		return ret;
	}

}

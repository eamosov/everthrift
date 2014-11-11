package com.knockchat.utils.timestat;

import java.util.WeakHashMap;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class TimeStat extends StatIF{
	
	WeakHashMap<Event, Event.EventState> events = new WeakHashMap<Event, Event.EventState>();
	
	Event event;
		
	public void start(){
		event.start();
	}
	
	public void end(){
		event.end();
	}
	
	public TimeStat(){
		reset();
		setDesc("TimeStat");
		event = createEvent("default");
	}
	
	public Event createEvent(){
		return createEvent(getDesc());
	}
	
	public Event createEvent(String aDesc){
		Event e = new Event();
		e.aTimeStat = this;
		e.desc = aDesc;
		synchronized(this){
			events.put(e, Event.EventState.STOP);
		}
		return e;
	}
	
	public static long getHRTime(){
		return System.nanoTime() / 1000;
	}
	
	/**
	 * 
	 * @param delta (in usec)
	 */
	synchronized public void update(long delta){
	
		if (delta==0)
			return;

		addValue(delta);
		
	}
	
	@Override
	String getStatsInternal(){
		String ret="";
		
		synchronized(this){
			ret = "<TS desc=\"" + getDesc() + "\" min=\"" + min + "\" max=\"" + max + "\" avr=\"" + avr +
					"\" count=\"" + count + "\" reset=\"" + autoReset + "\">\n";
			long now = getHRTime();
		
			for (Event i: events.keySet()){
				if (i.state == Event.EventState.RUN)
					ret += "\t<E desc=\"" + i.desc + "\" start=\"" + i.start + "\" duration=\"" + (now - i.start) + "\"/>\n";
			}
			
			ret += "</TS>";
		}
		return ret;				
	}
	
}

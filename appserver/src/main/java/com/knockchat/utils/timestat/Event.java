package com.knockchat.utils.timestat;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class Event {
		enum EventState { RUN, STOP}
		TimeStat aTimeStat;
		EventState state = EventState.STOP;
		long start = 0;
		long stop  = 0;
		String desc="";
		
		public String setDesc(String aDesc){
			String old = desc;
			desc = aDesc;
			return old;
		}
		
		public void start(){
			synchronized(this){
				start = TimeStat.getHRTime();
				state = EventState.RUN;
			}
		}
		
		public long end(){
			synchronized(this){
				if (state != EventState.RUN){
					System.err.println("TimeStat: end() without run(): desc=" + desc);
				}
				stop = TimeStat.getHRTime(); 
				state = EventState.STOP;
				long delta = stop - start;
				if (aTimeStat!=null) aTimeStat.update(delta);
				return delta;
			}
		}
}

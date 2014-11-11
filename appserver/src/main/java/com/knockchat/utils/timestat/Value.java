package com.knockchat.utils.timestat;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class Value extends StatIF {
		
		public void hit(long val){
			addValue(val);
		}
		
		@Override
		String getStatsInternal() {
			String ret = "<VAL desc=\"" + getDesc() + "\" min=\"" + min + "\" max=\"" + max + "\" avr=\"" + avr +
			"\" count=\"" + count + "\" reset=\"" + autoReset + "\"/>";
			return ret;
		}
}

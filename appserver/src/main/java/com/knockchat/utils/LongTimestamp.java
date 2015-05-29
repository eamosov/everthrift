package com.knockchat.utils;

import java.util.Date;

public class LongTimestamp {

	public static long now(){
		return System.currentTimeMillis();
	}

	public static long fromSecs(long secs){
		return secs * 1000;
	}
	
	public static long round100sec(long timestamp){
		return timestamp / 100000 * 100000;
	}
	
	public static long toSecs(long timestamp){
		return timestamp/1000;
	}
	
	public static long toMillis(long timestamp){
		return timestamp;
	}	
	
	public static long from(Date date){
		return date.getTime();
	}
	
}

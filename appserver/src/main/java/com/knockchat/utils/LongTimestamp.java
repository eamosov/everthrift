package com.knockchat.utils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;

public class LongTimestamp {
	
	public final static long MIN = 60L * 1000L;
	public final static long HOUR = MIN * 60;
	public final static long DAY = HOUR * 24L;
	public final static long MONTH = DAY * 30L;
	public final static long WEEK = DAY * 7L;
	
	public static final ZoneId systemDefault = ZoneId.systemDefault();
	public static final ZoneId gmt = ZoneId.of("GMT");
	
	public static final DateTimeFormatter formatterDateTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nx");
	public static final DateTimeFormatter formatterDate = DateTimeFormatter.ofPattern("yyyy-MM-dd");

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
	
	public static long dayStart(long timestamp, ZoneId zoneId){
		return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId).truncatedTo(ChronoUnit.DAYS).toEpochSecond() * 1000L;
	}

	public static long dayStart(long timestamp){
		return dayStart(timestamp, systemDefault);
	}

	public static long dayStartGmt(long timestamp){
		return dayStart(timestamp, gmt);
	}
	
	public static String formatDateTime(long timestamp, ZoneId zoneId){
		final ZonedDateTime dt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId);		
		return dt.format(formatterDateTime);			
	}

	public static String formatDate(long timestamp, ZoneId zoneId){
		final ZonedDateTime dt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId);		
		return dt.format(formatterDate);			
	}

}

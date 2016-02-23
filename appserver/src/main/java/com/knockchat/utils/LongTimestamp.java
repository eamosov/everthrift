package com.knockchat.utils;

import java.time.Instant;
import java.time.LocalDate;
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
	
	public static final ZoneId[] zones = new ZoneId[25];
	
	static {
		for (int i=-12; i<=12; i++){
			zones[i+12] = ZoneId.of("GMT" + (i>=0 ? "+" : "") + i);
		}
	}
		
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

	public static long roundSec(long timestamp){
		return timestamp / 1000 * 1000;
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
	
	public static ZoneId zoneOf(int offset){
		if (offset <-12 || offset > 12)
			throw new IllegalArgumentException("zone must be in [-12; 12]");
		
		return zones[offset+12];
	}
	
	public static long dayStart(long timestamp, int zoneOffset){
		return dayStart(timestamp, zoneOf(zoneOffset));
	}
	
	public static long dayStart(long timestamp, ZoneId zoneId){
		return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId).truncatedTo(ChronoUnit.DAYS).toEpochSecond() * 1000L;
	}

	/**
	 *  timestamp2 - timestamp1 in days
	 * @param timestamp1
	 * @param timestamp2
	 * @param zoneId
	 * @return
	 */
	public static long days(long timestamp1, long timestamp2, ZoneId zoneId){
		
		final LocalDate d2 = LocalDate.from(ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp2), zoneId));
		final LocalDate d1 = LocalDate.from(ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp1), zoneId));
		
		return d2.toEpochDay() - d1.toEpochDay();
	}

	public static long dayStart(long timestamp){
		return dayStart(timestamp, systemDefault);
	}

	public static long dayStartGmt(long timestamp){
		return dayStart(timestamp, zoneOf(0));
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

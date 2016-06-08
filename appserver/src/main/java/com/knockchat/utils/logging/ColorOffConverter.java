package com.knockchat.utils.logging;

import com.knockchat.sql.migration.logging.ConsoleColorer;

import ch.qos.logback.classic.pattern.ClassicConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class ColorOffConverter extends ClassicConverter {
	
	private static final String COLOR_RESET = ConsoleColorer.restore();
	
	@Override
	public String convert(ILoggingEvent e) {
		return COLOR_RESET;
	}

}


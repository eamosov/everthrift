package org.everthrift.sql.migrator;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.pattern.ClassicConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * @author efreet (Amosov Evgeniy)
 */
public class ColorOnConverter extends ClassicConverter {

    private static final String COLOR_DEBUG = ConsoleColorer.color(ConsoleColorer.CYAN);

    private static final String COLOR_INFO = ConsoleColorer.color(ConsoleColorer.GREEN);

    private static final String COLOR_WARN = ConsoleColorer.color(ConsoleColorer.YELLOW);

    private static final String COLOR_ERROR = ConsoleColorer.color(ConsoleColorer.RED);

    @Override
    public String convert(ILoggingEvent e) {
        switch (e.getLevel().levelInt) {
            case Level.DEBUG_INT:
                return COLOR_DEBUG;
            case Level.INFO_INT:
                return COLOR_INFO;
            case Level.WARN_INT:
                return COLOR_WARN;
            case Level.ERROR_INT:
                return COLOR_ERROR;
            default:
                return "";
        }
    }
}

package com.knockchat.appserver.cluster;

import org.jgroups.logging.Log;
import org.slf4j.Logger;

public class Slf4jLogImpl implements Log {
	
	private final Logger log;

	public Slf4jLogImpl(Logger log) {
		super();
		this.log = log;
	}

	@Override
	public boolean isFatalEnabled() {
		return log.isErrorEnabled();
	}

	@Override
	public boolean isErrorEnabled() {
		return log.isErrorEnabled();
	}

	@Override
	public boolean isWarnEnabled() {
		return log.isWarnEnabled();
	}

	@Override
	public boolean isInfoEnabled() {
		return log.isInfoEnabled();
	}

	@Override
	public boolean isDebugEnabled() {
		return log.isDebugEnabled();
	}

	@Override
	public boolean isTraceEnabled() {
		return log.isTraceEnabled();
	}

	@Override
	public void fatal(String msg) {
		log.error(msg);
	}

	@Override
	public void fatal(String msg, Object... args) {
		log.error(msg.replace("%s", "{}").replace("%d", "{}"), args);
	}

	@Override
	public void fatal(String msg, Throwable throwable) {
		log.error(msg, throwable);		
	}

	@Override
	public void error(String msg) {
		log.error(msg);		
	}

	@Override
	public void error(String format, Object... args) {
		log.error(format.replace("%s", "{}").replace("%d", "{}"), args);		
	}

	@Override
	public void error(String msg, Throwable throwable) {
		log.error(msg, throwable);		
	}

	@Override
	public void warn(String msg) {
		log.error(msg);		
	}

	@Override
	public void warn(String format, Object... args) {
		log.error(format.replace("%s", "{}").replace("%d", "{}"), args);		
	}

	@Override
	public void warn(String msg, Throwable throwable) {
		log.error(msg, throwable);		
	}

	@Override
	public void info(String msg) {
		log.error(msg);		
	}

	@Override
	public void info(String format, Object... args) {
		log.error(format.replace("%s", "{}").replace("%d", "{}"), args);		
	}

	@Override
	public void debug(String msg) {
		log.error(msg);		
	}

	@Override
	public void debug(String format, Object... args) {
		log.error(format.replace("%s", "{}").replace("%d", "{}"), args);		
	}

	@Override
	public void debug(String msg, Throwable throwable) {
		log.error(msg, throwable);		
	}

	@Override
	public void trace(Object msg) {
		log.trace("{}", msg);		
	}

	@Override
	public void trace(String msg) {
		log.trace(msg);		
	}

	@Override
	public void trace(String msg, Object... args) {
		log.trace(msg.replace("%s", "{}").replace("%d", "{}"), args);		
	}

	@Override
	public void trace(String msg, Throwable throwable) {
		log.trace(msg, throwable);
	}

	@Override
	public void setLevel(String level) {
				
	}

	@Override
	public String getLevel() {
		return "info";
	}

}

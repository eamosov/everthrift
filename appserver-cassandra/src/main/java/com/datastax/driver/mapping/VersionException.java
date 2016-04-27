package com.datastax.driver.mapping;

public class VersionException extends Exception {
	
	private static final long serialVersionUID = 1895234884439169934L;
	
	private final Object version;

	public VersionException(Object version) {
		this.version = version;
	}

	public Object getVersion() {
		return version;
	}

	@Override
	public String toString() {
		return "VersionException [version=" + version + "]";
	}	
}

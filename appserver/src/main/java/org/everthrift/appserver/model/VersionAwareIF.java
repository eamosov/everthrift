package org.everthrift.appserver.model;

public interface VersionAwareIF extends DaoEntityIF {	
	long getVersion();
}

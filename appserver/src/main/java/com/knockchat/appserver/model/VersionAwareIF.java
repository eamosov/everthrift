package com.knockchat.appserver.model;

public interface VersionAwareIF extends DaoEntityIF {	
	long getVersion();
}

package com.knockchat.appserver.model;

import com.knockchat.hibernate.dao.DaoEntityIF;

public interface VersionAwareIF extends DaoEntityIF {	
	long getVersion();
}

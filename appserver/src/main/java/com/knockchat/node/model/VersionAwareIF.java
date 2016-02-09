package com.knockchat.node.model;

import com.knockchat.hibernate.dao.DaoEntityIF;

public interface VersionAwareIF extends DaoEntityIF {	
	int getVersion();
}

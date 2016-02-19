package com.knockchat.appserver.transport.jgroups;

import org.jgroups.logging.CustomLogFactory;
import org.jgroups.logging.Log;
import org.slf4j.LoggerFactory;

import com.knockchat.appserver.cluster.Slf4jLogImpl;

public class JGroupsLogFactory implements CustomLogFactory {

	@Override
	public Log getLog(Class clazz) {
		return new Slf4jLogImpl(LoggerFactory.getLogger(clazz));
	}

	@Override
	public Log getLog(String category) {
		// TODO Auto-generated method stub
		return null;
	}

}
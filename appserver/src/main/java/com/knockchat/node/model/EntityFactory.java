package com.knockchat.node.model;

import org.apache.thrift.TException;

public interface EntityFactory<PK, ENTITY>{
	ENTITY create(PK id) throws TException;
}
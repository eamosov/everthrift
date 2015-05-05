package com.knockchat.node.model;

public interface EntityFactory<PK, ENTITY>{
	ENTITY create(PK id);
}
package com.knockchat.node.model;

public interface ModelFactoryIF<PK, ENTITY> extends RwModelFactoryIF<PK, ENTITY> {
	
	ENTITY update(ENTITY e);
	
    boolean isUpdated();
}

package com.knockchat.node.model;


public interface RwModelFactoryIF<PK, ENTITY> extends RoModelFactoryIF<PK, ENTITY>{		            
        
    ENTITY insertEntity(ENTITY e);
            
    void deleteEntity(ENTITY e);        	
}

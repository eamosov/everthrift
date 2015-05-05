package com.knockchat.node.model;

public abstract class MutatorDecorator<ENTITY> implements EntityMutator<ENTITY>{
	
	protected final EntityMutator<ENTITY> delegate;
	
	public MutatorDecorator(EntityMutator<ENTITY> e){
		delegate = e;
	}    	
}
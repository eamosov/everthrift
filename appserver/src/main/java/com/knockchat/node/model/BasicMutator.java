package com.knockchat.node.model;

import org.apache.thrift.TException;

public abstract class BasicMutator<ENTITY> implements EntityMutator<ENTITY>{

	@Override
	public boolean beforeUpdate() throws TException {
		return true;
	}

	@Override
	public void afterTransactionClosed() {
	}
	
	@Override
	public void afterUpdate(){
		
	}
}
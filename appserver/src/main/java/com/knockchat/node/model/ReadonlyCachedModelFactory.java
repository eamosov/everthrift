package com.knockchat.node.model;

import java.io.Serializable;

import org.apache.commons.lang.NotImplementedException;

import com.knockchat.hibernate.dao.DummyEntity;

public abstract class ReadonlyCachedModelFactory<K,V> extends AbstractCachedModelFactory<K, V, Void, Serializable, DummyEntity> {

	public ReadonlyCachedModelFactory(String cacheName) {
		super(cacheName);
	}

	@Override
	public final DummyEntity updateEntity(DummyEntity e) {
		throw new NotImplementedException();
	}

	@Override
	public final void deleteEntity(DummyEntity e){
		throw new NotImplementedException();
	}
}

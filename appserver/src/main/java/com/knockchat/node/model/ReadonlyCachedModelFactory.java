package com.knockchat.node.model;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import com.knockchat.hibernate.dao.DummyEntity;

public abstract class ReadonlyCachedModelFactory<K,V> extends AbstractCachedModelFactory<K, V, Void, Serializable, DummyEntity> {

	public ReadonlyCachedModelFactory(String cacheName) {
		super(cacheName);
	}

	@Override
	protected final XAwareIF<K, V> getAwareAdapter(Void m) {
		throw new NotImplementedException();
	}

	@Override
	public final DummyEntity update(DummyEntity e) {
		throw new NotImplementedException();
	}

	@Override
	public final void update(List<DummyEntity> l) {
		throw new NotImplementedException();
	}

	@Override
	public final void update(DummyEntity[] l) {
		throw new NotImplementedException();
	}

}

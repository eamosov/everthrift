package com.knockchat.utils.initializer;

import java.util.concurrent.Callable;

import com.google.common.base.Throwables;

public class CallableInitializer<T> extends LazyInitializer<T> implements Initializer<T> {

	private static final long serialVersionUID = 1L;
	
	private final Callable<T> c;
	
	public CallableInitializer(Callable<T> c) {
		this.c = c;
	}

	@Override
	protected T initialize() {
		try {
			return c.call();
		} catch (Exception e) {
			throw Throwables.propagate(e);
		}
	}

}

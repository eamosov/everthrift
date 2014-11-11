package com.knockchat.proactor.handlers.mcb;

import java.util.concurrent.locks.Lock;

import com.knockchat.proactor.handlers.AbstractLockingHandler;
import com.knockchat.utils.meta.MetaMethod;


/**
 * @author efreet (Amosov Evgeniy)
 */
public class McbMethodHandler extends AbstractLockingHandler {

	private final Object target;
	private final MetaMethod method;

	public McbMethodHandler( MetaMethod method, Object target, Lock writeLock ) {
		super( writeLock );

		this.target = target;
		this.method = method;
	}

	@Override
	protected Object run( Object arg ) {
		return method.invoke( target, arg );
	}

}

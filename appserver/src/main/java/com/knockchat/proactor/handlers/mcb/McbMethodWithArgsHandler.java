package com.knockchat.proactor.handlers.mcb;

import java.util.concurrent.locks.Lock;

import com.knockchat.proactor.handlers.AbstractLockingHandler;
import com.knockchat.utils.meta.MetaMethod;


/**
 * @author efreet (Amosov Evgeniy)
 */
public class McbMethodWithArgsHandler extends AbstractLockingHandler {

	private final Object target;

	private final MetaMethod method;

	private final Object[] args;

	public McbMethodWithArgsHandler( MetaMethod method, Object target, Lock writeLock, Object[] args ) {
		super( writeLock );

		this.target = target;
		this.method = method;
		this.args = args;
	}

	@Override
	protected Object run( Object arg ) {
		Object[] methodArgs = new Object[args.length + 1];

		methodArgs[0] = arg;

		System.arraycopy( args, 0, methodArgs, 1, args.length );

		return method.invoke( target, methodArgs );
	}

}

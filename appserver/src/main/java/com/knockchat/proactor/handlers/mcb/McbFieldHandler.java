package com.knockchat.proactor.handlers.mcb;

import java.util.concurrent.locks.Lock;

import com.knockchat.proactor.handlers.AbstractLockingHandler;
import com.knockchat.utils.meta.MetaProperty;


/**
 * @author efreet (Amosov Evgeniy)
 */
public class McbFieldHandler extends AbstractLockingHandler {

	private final MetaProperty property;

	private final Object target;

	public McbFieldHandler( MetaProperty property, Object target, Lock writeLock ) {
		super(  writeLock );

		this.target = target;
		this.property = property;
	}

	@Override
	protected Object run( Object arg ) {
		property.set( target, arg );
		return arg;
	}

}

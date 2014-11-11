package com.knockchat.proactor.handlers;

import java.util.concurrent.locks.Lock;

/**
 * @author efreet (Amosov Evgeniy)
 */
public abstract class AbstractLockingHandler extends AbstractHandler {

	private final Lock lock;

	public AbstractLockingHandler( Lock lock ) {
		this.lock = lock;
	}

	@Override
	public final Object handle( Object arg ) {
		lock.lock(); // Блокируем контроллер

		try {
			return run( arg );
		} finally {
			lock.unlock();
		}
	}

	@Override
	public final boolean tryHandle( Object arg ) {
		if ( !lock.tryLock() ) // Пытаемся заблокировать контроллер
			return false;

		try {
			run( arg );
		} finally {
			lock.unlock();
		}

		return true;
	}

	protected abstract Object run( Object arg );

}

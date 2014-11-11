package com.knockchat.proactor.handlers;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author efreet (Amosov Evgeniy)
 */
public abstract class AbstractHandler implements Handler {

	private static final class Finalizer{
		
		private Finalizer(){
			aliveCount.incrementAndGet();
		}
		
		@Override
		protected void finalize() throws Throwable {
			aliveCount.decrementAndGet();
		}		
	}

	@SuppressWarnings("unused")
	private static final Logger log = LoggerFactory.getLogger( AbstractHandler.class );
	
	@SuppressWarnings("unused")
	private final Finalizer finalizer;

	public AbstractHandler() {
		finalizer = new Finalizer();
	}

	@Override
	public final Callable toCallable( final Object arg ) {
		return new Callable() {

			@Override
			public Object call() {
				return handle( arg );
			}

		};
	}

	@Override
	public boolean tryHandle( Object arg ) {
		handle( arg );

		return true;
	}
}

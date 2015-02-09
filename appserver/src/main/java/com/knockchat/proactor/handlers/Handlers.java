package com.knockchat.proactor.handlers;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

/**
 * @author efreet (Amosov Evgeniy)
 */
public class Handlers {

	/**
	 * Логгер
	 */
	private static final Logger log = LoggerFactory.getLogger( Handlers.class );

	/**
	 * Список локаторов фабрик, вызываемых по очереди
	 * TODO: Оптимизация - использовать обычный массив
	 */
	private static final List<HandlerFactory> factories = new LinkedList<HandlerFactory>();

	/**
	 * Получить фабрику обработчиков для заданного класса контроллера
	 * 
	 * @param controllerClass
	 *            класс контроллера
	 * @return объект фабрики обработчиков
	 */
	@SuppressWarnings("unchecked")
	public static HandlerFactory getFactory( Class controllerClass ) {
		for ( HandlerFactory factory : factories )
			if ( factory.accepts( controllerClass ) )
				return factory; // Если нашли фабрику, отдаем ее

		log.warn( "No handler factory found for controllerCls class {}", controllerClass );

		return null; // Ничего не нашли
	}
	
	public static <F,T> Function<F,T> toFunction(final Object o, final String fname){
		final Lock l = new ReentrantLock();
		return new Function<F,T>(){

			@Override
			public T apply(F input) {
				return (T)getFactory(o.getClass()).get(o, fname, l).handle(input);
			}};
	}

	/**
	 * Зарегистрировать новую фабрику обработчиков
	 * 
	 * @param fcatory
	 *            фабрика
	 */
	public static void registerFactory( HandlerFactory factory ) {
		if (!factories.contains(factory))
			factories.add( factory );
		
	}

	/**
	 * Удалить зарегистрированную фабрику обработчиков
	 * 
	 * @param faсtory
	 *            фабрика
	 */
	public static boolean unregisterFactoryLocator( HandlerFactory factory ) {
		return factories.remove( factory );
	}

	/**
	 * Очистить список фабрик обработчиков
	 */
	public static void unregisterAllFactories() {
		factories.clear();
	}
}

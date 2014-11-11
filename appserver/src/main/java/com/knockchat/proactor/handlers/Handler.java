package com.knockchat.proactor.handlers;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author efreet (Amosov Evgeniy)
 */
public interface Handler {

	/**
	 * Количество обработчиков, находящихся в памяти
	 */
	AtomicLong aliveCount = new AtomicLong( 0 );

	/**
	 * Обработать событие
	 *
	 * @param arg
	 *            аргумент, передаваемый в обработчик события
	 */
	Object handle( Object arg );

	/**
	 * Попытаться обработать событие
	 *
	 * @param arg
	 *            аргумент, передаваемый в обработчик события
	 * @return true, если обработка была начата, false если обработка не может
	 *         быть выполнена в настоящий момент (например, обработчик
	 *         заблокирован)
	 */
	boolean tryHandle( Object arg );

	/**
	 * Преобразовать обработчик к объекту Runnable, выполнение которого
	 * осуществляет обработку события с заданным аргументом
	 *
	 * @param arg
	 *            аргумент, передаваемый в обработчик события
	 * @return объект Runnable
	 */
	Callable toCallable( final Object arg );
}

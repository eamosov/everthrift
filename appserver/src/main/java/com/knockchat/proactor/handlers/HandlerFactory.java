package com.knockchat.proactor.handlers;

import java.util.concurrent.locks.Lock;

/**
 * @author efreet (Amosov Evgeniy)
 */
public interface HandlerFactory {

	/**
	 * Получить обработчик для события контроллера. В метод передается защелка
	 * записи, которая всегда постоянна для одного и того же контроллера. Таким
	 * образом, обработчик идентифицируется парой контроллер-событие, что может
	 * быть использовано для оптимизациии внутри фабрики.
	 *
	 * @param controller
	 *            контроллер
	 * @param event
	 *            событие
	 * @param writeLock
	 *            защелка записи
	 * @return обработчик
	 */
	Handler get( Object controller, String event, Lock writeLock );

	/**
	 * Получитьп параметризованный обработчик для события контроллера. В метод
	 * передается защелка записи, которая всегда постоянна для одного и того же
	 * контроллера. Таким образом, обработчик идентифицируется парой
	 * контроллер-событие, что может быть использовано для оптимизациии внутри
	 * фабрики.
	 *
	 * @param controller
	 *            контроллер
	 * @param event
	 *            событие
	 * @param writeLock
	 *            защелка записи
	 * @param args
	 *            дополнительные аргументы обработчика
	 * @return обработчик
	 */
	Handler get( Object controller, String event, Lock writeLock, Object... args );

	/**
	 * Проверить, создает ли фабрика обработчики для контроллеров заданного
	 * типа.
	 *
	 * @param controllerClass
	 *            класс контроллера
	 * @return true, если фабрика создает обработчики для таких контроллеров.
	 */
	@SuppressWarnings("unchecked")
	boolean accepts( Class clazz );
}

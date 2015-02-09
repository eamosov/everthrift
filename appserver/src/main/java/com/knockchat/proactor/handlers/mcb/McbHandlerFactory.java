package com.knockchat.proactor.handlers.mcb;

import java.util.concurrent.locks.Lock;

import com.knockchat.proactor.handlers.Handler;
import com.knockchat.proactor.handlers.HandlerFactory;
import com.knockchat.utils.meta.MetaClass;
import com.knockchat.utils.meta.MetaClasses;
import com.knockchat.utils.meta.MetaMethod;
import com.knockchat.utils.meta.MetaProperty;


/**
 * Фабрика обработчиков, построенная на основе метаклассов (Meta Class Based).
 * Генерируемые обработчики используют метакласс контроллера для доступа к его
 * полям и методам.
 *
 * @author efreet
 *
 */
public class McbHandlerFactory implements HandlerFactory {

	@SuppressWarnings("unchecked")
	@Override
	public boolean accepts( Class controllerClass ) {
		return true; // Фабрика работает для всех типов контроллеров
	}

	@Override
	public Handler get( Object controller, String event, Lock writeLock ) {
		MetaClass mc = MetaClasses.get( controller.getClass() );

		MetaProperty field = mc.getProperty( event );
		if ( field != null ) // Событие - установка поля
			return new McbFieldHandler( field, controller, writeLock );

		MetaMethod method = mc.getMethod( event );
		if ( method != null ) // Событие - вызов метода
			return new McbMethodHandler( method, controller, writeLock );

		return null;
	}

	@Override
	public Handler get( Object controller, String event, Lock writeLock, Object... args ) {
		if ( args.length == 0 )
			return get(controller,event,writeLock);

		MetaClass mc = MetaClasses.get( controller.getClass() );

		MetaMethod method = mc.getMethod( event );
		if ( method != null ) // Событие - вызов метода
			return new McbMethodWithArgsHandler( method, controller, writeLock, args );

		throw new Error("Unknown handler for event '" + event + "' in controllerCls " + mc.getName() );
	}
}

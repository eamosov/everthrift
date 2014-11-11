package com.knockchat.proactor.handlers.mcb;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import com.knockchat.proactor.handlers.Handler;
import com.knockchat.proactor.handlers.HandlerFactory;
import com.knockchat.utils.Pair;
import com.knockchat.utils.meta.MetaClass;
import com.knockchat.utils.meta.MetaClasses;
import com.knockchat.utils.meta.MetaMethod;
import com.knockchat.utils.meta.MetaProperty;


/**
 * @author efreet (Amosov Evgeniy)
 */
public class CachedMcbHandlerFactory implements HandlerFactory {

	private final Map<Pair<String, String>, Object> eventMap = new HashMap<Pair<String, String>, Object>();

	@SuppressWarnings("unchecked")
	@Override
	public boolean accepts( Class clazz ) {
		return true; // Фабрика работает для всех типов контроллеров
	}

	@Override
	public Handler get( Object controller, String event, Lock writeLock ) {
		Object o = getEventFromMap( controller, event );

		if ( o instanceof MetaProperty ) // Событие - установка поля
			return new McbFieldHandler( (MetaProperty) o, controller, writeLock );

		if ( o instanceof MetaMethod ) // Событие - вызов метода
			return new McbMethodHandler( (MetaMethod) o, controller, writeLock );
		
		throw new RuntimeException( "Unknown event type: " + o.getClass() );
	}
	
	@Override
	public Handler get( Object controller, String event, Lock writeLock, Object... args ) {
		if ( args.length == 0 )
			return get( controller, event, writeLock );

		Object o = getEventFromMap( controller, event );
		
		if ( o instanceof MetaMethod ) // Событие - вызов метода
			return new McbMethodWithArgsHandler( (MetaMethod) o, controller, writeLock, args );
		
		if ( o instanceof MetaProperty )
			throw new RuntimeException( "Cannot handler field event with additional arguments: " + o.getClass() );

		throw new RuntimeException( "Unknown event type: " + o.getClass() );
	}

	private Object getEventFromMap( Object controller, String event ) {		
		Pair<String, String> key = new Pair<String, String>( controller.getClass().getName(), event );
		Object o = eventMap.get( key );

		if ( o == null ) {
			MetaClass mc = MetaClasses.get( controller.getClass() ); // Метакласс контроллера
			
			o = mc.getFieldProperty( event );

			if ( o == null )
				o = mc.getMethod( event );

			if ( o == null )
				throw new Error( "Unknown handler for event '" + event + "' in controller " + controller.getClass().getName() );
				
			eventMap.put( key, o );
		}

		return o;
	}

}

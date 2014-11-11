package com.knockchat.utils.meta.getset;

import com.knockchat.utils.meta.MetaMethod;
import com.knockchat.utils.meta.MetaProperty;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class GetSetProperty implements MetaProperty{
	
	private final String name;	
	private final Class<?> type;
	
	private final MetaMethod setter;
	private final MetaMethod getter;
	
	public GetSetProperty( String name, MetaMethod getter, MetaMethod setter ) {
		this.name = name;
		this.setter = setter;
		this.getter = getter;
		this.type = getter == null ? null : getter.getReturnType(); 
	}

	@Override
	public Object get( Object target ) {
		if ( getter == null )
			throw new RuntimeException( "Property " + name + " is not readable" ); 
			
		return getter.invoke( target );
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void set( Object target, Object value ) {
		if ( setter == null )
			throw new RuntimeException( "Property " + name + " is not writable" ); 
			
		setter.invoke( target, value );
	}

	@Override
	public Class<?> getType() {
		return type;
	}

}

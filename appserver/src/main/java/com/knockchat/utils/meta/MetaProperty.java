package com.knockchat.utils.meta;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public interface MetaProperty {
	
	public Class<?> getType();
	
	public String getName();

	public Object get( Object target );
	
	public void set( Object target, Object value );

}

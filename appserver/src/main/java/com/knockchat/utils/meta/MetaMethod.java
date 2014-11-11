package com.knockchat.utils.meta;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public interface MetaMethod {
	
	public Class<?> getReturnType();
	
	public String getName();
	
	public String getSignature();

	public Object invoke( Object target, Object... args );
}

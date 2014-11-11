package com.knockchat.utils.meta.asm;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class DefiningClassLoader extends ClassLoader {

	private final AtomicLong classCounter = new AtomicLong();

	public DefiningClassLoader( ClassLoader parent ) {
		super( parent );
	}

	public long nextClassIndex() {
		return classCounter.getAndIncrement();
	}

	public Class<?> define(String name, byte[] bytecode) {
		return defineClass( name, bytecode, 0, bytecode.length );
	}
}

package com.knockchat.utils;

import java.lang.reflect.Method;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class JvmNotation {

	public static String getJvmSignature( Method method ) {
		StringBuilder b = new StringBuilder();
		appendJvmSignature( method, b );
		return b.toString();
	}

	public static void appendJvmSignature( Method method, StringBuilder b ) {
		b.append( '(' );
		for ( Class<?> argType :method.getParameterTypes() )
			appendJvmType( b, argType );
		b.append( ')' );
		appendJvmType( b, method.getReturnType() );
	}

	public static String getJvmType( Class<?> clazz ) {
		StringBuilder b = new StringBuilder();
		appendJvmType( b, clazz );
		return b.toString();
	}

	public static void appendJvmType( StringBuilder b, Class<?> clazz ) {
		if ( clazz.isArray() ) {
			b.append( '[' );
			appendJvmType( b, clazz.getComponentType() );
		} else if ( !clazz.isPrimitive() ) {
			b.append( 'L' );
			b.append( clazz.getName().replace( '.', '/' ) );
			b.append( ';' );
		} else if ( clazz.equals( Byte.TYPE )){
			b.append( 'B' );
		} else if ( clazz.equals( Short.TYPE )){
			b.append( 'S' );
		} else if ( clazz.equals( Integer.TYPE )){
			b.append( 'I' );
		} else if ( clazz.equals( Long.TYPE )){
			b.append( 'J' );
		} else if ( clazz.equals( Float.TYPE )){
			b.append( 'F' );
		} else if ( clazz.equals( Double.TYPE )){
			b.append( 'D' );
		} else if ( clazz.equals( Boolean.TYPE )){
			b.append( 'Z' );
		} else if ( clazz.equals( Character.TYPE )){
			b.append( 'C' );
		} else if ( clazz.equals( Void.TYPE )){
			b.append( 'V' );
		} else {
			throw new Error( "Unknown primitive type " + clazz );
		}
	}

	public static String getJvmClass( Class<?> clazz ) {
		if ( clazz.isPrimitive())
			throw new IllegalArgumentException( "Cann't get class for primitive type " + clazz.getName() );

		return clazz.getName().replace( '.', '/' );
	}

	public static void appednJvmClass( StringBuilder b, Class<?> clazz ) {
		if ( clazz.isPrimitive())
			throw new IllegalArgumentException( "Cann't get class for primitive type " + clazz.getName() );

		b.append( clazz.getName().replace( '.', '/' ) );
	}

	public static String getJavaClass( String className ) {
		if ( className.charAt( className.length() - 1 ) == ';' )
			className = className.substring( 1, className.length() - 1 );

		return className.replace( '/', '.' );
	}

}

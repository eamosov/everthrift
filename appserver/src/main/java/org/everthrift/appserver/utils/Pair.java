package org.everthrift.appserver.utils;

import java.io.Serializable;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class Pair<T, V> implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5839335947801602957L;
	public final T first;
	public final V second;
	
	public static <T,V> Pair<T, V> create(T first, V second){
		return new Pair<T,V>(first, second);
	}

	public Pair( T first, V second ) {
		this.first = first;
		this.second = second;
	}
	
	public T getFirst(){
		return first;
	}

	public V getSecond(){
		return second;
	}

	@Override
	public int hashCode() {
		return ( first == null ? 11 : first.hashCode() ) + ( second == null ? 35 : second.hashCode() );
	}
	
	@Override
	public boolean equals( Object o ) {
		if ( o == null ) return false;
		if ( !(o instanceof Pair ) ) return false;
		
		Pair<?,?> p = (Pair<?,?>)o;
		
		boolean ef = first == null ? p.first == null : first.equals( p.first );
		boolean es = second == null ? p.second == null : second.equals( p.second );
		
		return ef && es;
	}
	
	@Override
	public String toString(){
		return "first='" + first.toString() + "', second='" + second.toString() + "'";
	}
}

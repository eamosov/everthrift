package org.everthrift.utils;

import java.io.Serializable;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class Three<T, V, K> implements Serializable {
	
	private static final long serialVersionUID = 5839335947801602957L;
	
	public final T first;
	public final V second;
	public final K third;
	
	public static <T, V, K> Three<T, V, K> create(T first, V second, K third){
		return new Three<T, V, K>(first, second, third);
	}

	public Three( T first, V second, K third ) {
		this.first = first;
		this.second = second;
		this.third = third;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		result = prime * result + ((third == null) ? 0 : third.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Three other = (Three) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		if (third == null) {
			if (other.third != null)
				return false;
		} else if (!third.equals(other.third))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Three [first=" + first + ", second=" + second + ", third=" + third + "]";
	}
	
}

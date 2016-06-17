package org.everthrift.utils;

public interface Function2<F1, F2, T> {
	
	  T apply(F1 input1, F2 input2);
	  
	  @Override
	  boolean equals(Object object);
}

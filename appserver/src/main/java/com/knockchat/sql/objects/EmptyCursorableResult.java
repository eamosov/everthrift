package com.knockchat.sql.objects;

import java.util.Iterator;

public class EmptyCursorableResult<T> implements CursorableResult<T> {
	
	public static final EmptyCursorableResult INSTANCE = new EmptyCursorableResult();

	@Override
	public Iterator<T> iterator() {
		return new Iterator<T>(){

			@Override
			public boolean hasNext() {
				return false;
			}

			@Override
			public T next() {
				return null;
			}

			@Override
			public void remove() {
			}};
	}

	@Override
	public void close() {
	}

}

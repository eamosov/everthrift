package com.knockchat.sql.objects;


public interface CursorableResult<T> extends Iterable<T> {

	void close();

}

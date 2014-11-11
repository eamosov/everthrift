package com.knockchat.sql;

public class SqlMapperException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public SqlMapperException() {
		super();
	}

	public SqlMapperException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public SqlMapperException(String message, Throwable cause) {
		super(message, cause);
	}

	public SqlMapperException(String message) {
		super(message);
	}

	public SqlMapperException(Throwable cause) {
		super(cause);
	}
}

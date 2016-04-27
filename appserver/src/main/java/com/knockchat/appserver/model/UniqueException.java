package com.knockchat.appserver.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UniqueException extends RuntimeException{
	
	
	private static final long serialVersionUID = 1L;
	private static final Logger log = LoggerFactory.getLogger(UniqueException.class);
	
	private final String fieldName;
	private final boolean isPrimaryKey;	
	
	public UniqueException(String message, String fieldName){
		super(message);
		this.fieldName = fieldName;
		this.isPrimaryKey = false;
	}
	
	public UniqueException(String fieldName, boolean isPrimaryKey, Exception e){
		super(e);
		this.fieldName = fieldName;
		this.isPrimaryKey = isPrimaryKey;
	}

	public UniqueException(String fieldName, Exception e) {
		this(fieldName, false, e);
	}

	public String getFieldName() {
		return fieldName;
	}
	
	public boolean isPrimaryKey(){
		return isPrimaryKey;
	}
	
}

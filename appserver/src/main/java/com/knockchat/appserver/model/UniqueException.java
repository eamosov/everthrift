package com.knockchat.appserver.model;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hibernate.exception.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UniqueException extends RuntimeException{
	
	
	private static final long serialVersionUID = 1L;
	private static final Logger log = LoggerFactory.getLogger(UniqueException.class);
	
	private static final Pattern pkey = Pattern.compile("^[^_]+_pkey$");
	private static final Pattern p = Pattern.compile("^[^_]+_([^_]+)_[^_]+$");

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
	
	public UniqueException(ConstraintViolationException e){
		super(e);
		
		if (e.getConstraintName() != null){
			
			final Matcher pm = pkey.matcher(e.getConstraintName());
			if (pm.matches()){
				this.fieldName = null;
				this.isPrimaryKey = true;
			}else{
				this.isPrimaryKey = false;
				final Matcher m = p.matcher(e.getConstraintName());
				if (m.matches()){
					this.fieldName = m.group(1);
				}else{
					log.error("Coudn't parse constraint name:{}", e.getConstraintName());
					this.fieldName = null;
				}							
			}
			
		}else{
			this.fieldName = null;
			this.isPrimaryKey = false;
		}
	}

	public String getFieldName() {
		return fieldName;
	}
	
	public boolean isPrimaryKey(){
		return isPrimaryKey;
	}
	
}

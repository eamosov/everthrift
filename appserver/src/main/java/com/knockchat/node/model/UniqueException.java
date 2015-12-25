package com.knockchat.node.model;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hibernate.exception.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UniqueException extends RuntimeException{
	
	
	private static final long serialVersionUID = 1L;
	private static final Logger log = LoggerFactory.getLogger(UniqueException.class);
	
	private static final Pattern p = Pattern.compile("^[^_]+_([^_]+)_[^_]+$");
	
	private final String fieldName;

	public UniqueException(String fieldName, Exception e) {
		super(e);
		this.fieldName = fieldName;
	}
	
	public UniqueException(ConstraintViolationException e){
		super(e);
		
		if (e.getConstraintName() != null){
			final Matcher m = p.matcher(e.getConstraintName());
			if (m.matches()){
				this.fieldName = m.group(1);
			}else{
				log.error("Coudn't parse constraint name:{}", e.getConstraintName());
				this.fieldName = null;
			}			
		}else{
			this.fieldName = null;
		}
	}

	public String getFieldName() {
		return fieldName;
	}	
	
}

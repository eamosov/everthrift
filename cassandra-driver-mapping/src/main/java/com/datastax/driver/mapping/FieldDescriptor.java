package com.datastax.driver.mapping;

import java.lang.reflect.Field;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.mapping.ColumnMapper.Kind;
import com.google.common.reflect.TypeToken;

public class FieldDescriptor {
	
	private String columnName;
	private String fieldName;
	private Kind kind;
	private TypeToken<Object> fieldType;
	private TypeCodec<Object> customCodec;

    public static String newAlias(FieldDescriptor field, int columnNumber) {
        return "col" + columnNumber;

    }
    
    public FieldDescriptor(String columnName, String fieldName, Kind kind, TypeToken<Object> fieldType, TypeCodec<Object> customCodec) {
		super();
		this.columnName = columnName;
		this.fieldName = fieldName;
		this.kind = kind;
		this.fieldType = fieldType;
		this.customCodec = customCodec;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public Kind getKind() {
		return kind;
	}

	public void setKind(Kind kind) {
		this.kind = kind;
	}

	public TypeCodec<Object> getCustomCodec() {
		return customCodec;
	}

	public void setCustomCodec(TypeCodec<Object> customCodec) {
		this.customCodec = customCodec;
	}

	public TypeToken<Object> getFieldType() {
		return fieldType;
	}

	public void setFieldType(TypeToken<Object> fieldType) {
		this.fieldType = fieldType;
	}

}

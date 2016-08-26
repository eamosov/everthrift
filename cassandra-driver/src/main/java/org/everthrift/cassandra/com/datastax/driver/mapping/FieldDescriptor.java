package org.everthrift.cassandra.com.datastax.driver.mapping;

import com.datastax.driver.core.TypeCodec;
import com.google.common.reflect.TypeToken;
import org.everthrift.cassandra.com.datastax.driver.mapping.ColumnMapper.Kind;
import org.everthrift.cassandra.com.datastax.driver.mapping.EntityMapper.ColumnScenario;

public class FieldDescriptor {

    private String columnName;

    private String fieldName;

    private Kind kind;

    private TypeToken<Object> fieldType;

    private TypeCodec<Object> customCodec;

    private ColumnScenario columnScenario;

    public static String newAlias(FieldDescriptor field, int columnNumber) {
        return "col" + columnNumber;

    }

    public FieldDescriptor(String columnName, String fieldName, Kind kind, ColumnScenario columnScenario, TypeToken<Object> fieldType,
                           TypeCodec<Object> customCodec) {
        super();
        this.columnName = columnName;
        this.fieldName = fieldName;
        this.kind = kind;
        this.fieldType = fieldType;
        this.customCodec = customCodec;
        this.columnScenario = columnScenario;
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

    public ColumnScenario getColumnScenario() {
        return columnScenario;
    }

    public void setColumnScenario(ColumnScenario columnScenario) {
        this.columnScenario = columnScenario;
    }

}

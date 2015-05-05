package com.knockchat.hibernate.model;

import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

public class Table {

    protected PrimaryKey primaryKey;
    protected Map<String, Column> columnsByName;
    protected String schema;
    protected String tableName;
    protected Class javaClass;
    protected boolean view = false;

    public Table(String schema, String tableName, Class<?> javaClass, List<Column> columns, PrimaryKey pk, boolean view) {
        this.view = view;
        this.schema = schema;
        this.tableName = tableName;
        this.javaClass = javaClass;
        this.primaryKey = pk;
        columnsByName = Maps.uniqueIndex(columns, new Function<Column, String>() {
            @Override
            public String apply(Column input) {
                return input.getColumnName();
            }
        });        
    }

    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
    }

    public Map<String, Column> getColumnsByName() {
        return columnsByName;
    }

    public void addColumns(List<Column> columns) {
        for (Column col : columns)
            columnsByName.put(col.getColumnName(),col);
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchema() {
        return schema;
    }

    public Class getJavaClass() {
        return javaClass;
    }

    public boolean isView() {
        return view;
    }

    public boolean isValid(){
        return this.primaryKey != null && this.primaryKey.getColumnNames().size() > 0 &&
                this.columnsByName.keySet().containsAll(this.primaryKey.getColumnNames());
    }
}

package com.knockchat.hibernate.model;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

public class Column {

    protected String columnName;
    protected int jdbcType;
    protected String  columnType;
    protected boolean nullable;
    protected Integer length;
    protected Integer scale;
    protected Class javaClass;
    protected String propertyName;
    protected boolean isAutoincrement;	

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public void setJdbcType(int jdbcType) {
        this.jdbcType = jdbcType;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public Class getJavaClass() {
        return javaClass;
    }

    public void setJavaClass(Class javaClass) {
        this.javaClass = javaClass;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    public boolean isValid(){
        return this.propertyName!=null && this.javaClass != null;
    }

    public static class ColumnExtractor implements ResultSetExtractor<List<Column>> {

        @Override
        public List<Column> extractData(ResultSet rs) throws SQLException, DataAccessException {
            List<Column> columns = new ArrayList<>();
            while(rs.next()){
                Column column = new Column();
                column.setColumnName(rs.getString("COLUMN_NAME"));
                column.setJdbcType(rs.getInt("DATA_TYPE"));
                column.setLength(rs.getInt("COLUMN_SIZE"));
                column.setScale(rs.getInt("DECIMAL_DIGITS"));
                column.setNullable(rs.getInt("NULLABLE") > 0);
                column.setColumnType(rs.getString("TYPE_NAME"));
                column.setAutoincrement(rs.getString("IS_AUTOINCREMENT").equals("YES"));
                columns.add(column);
            }
            return columns;
        }
    }

	public boolean isAutoincrement() {
		return isAutoincrement;
	}

	public void setAutoincrement(boolean isAutoincrement) {
		this.isAutoincrement = isAutoincrement;
	}

	@Override
	public String toString() {
		return "Column [columnName=" + columnName + ", jdbcType=" + jdbcType
				+ ", columnType=" + columnType + ", nullable=" + nullable
				+ ", length=" + length + ", scale=" + scale + ", javaClass="
				+ javaClass + ", propertyName=" + propertyName
				+ ", isAutoincrement=" + isAutoincrement + "]";
	}

}

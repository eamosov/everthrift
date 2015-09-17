package com.knockchat.hibernate.model;

import gnu.trove.map.hash.TLongLongHashMap;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.hibernate.type.BigDecimalType;
import org.hibernate.type.BigIntegerType;
import org.hibernate.type.BooleanType;
import org.hibernate.type.ByteType;
import org.hibernate.type.CharacterType;
import org.hibernate.type.DateType;
import org.hibernate.type.DoubleType;
import org.hibernate.type.FloatType;
import org.hibernate.type.IntegerType;
import org.hibernate.type.LongType;
import org.hibernate.type.NumericBooleanType;
import org.hibernate.type.ShortType;
import org.hibernate.type.StringType;
import org.hibernate.type.TimeType;
import org.hibernate.type.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import com.knockchat.hibernate.ThriftPropertyAccessStrategy;
import com.knockchat.hibernate.model.types.BoxType;
import com.knockchat.hibernate.model.types.CustomTypeFactory;
import com.knockchat.hibernate.model.types.DoubleListType;
import com.knockchat.hibernate.model.types.IntegerListType;
import com.knockchat.hibernate.model.types.JsonType;
import com.knockchat.hibernate.model.types.LongDateType;
import com.knockchat.hibernate.model.types.LongIntervalType;
import com.knockchat.hibernate.model.types.LongListType;
import com.knockchat.hibernate.model.types.LongLongHstoreType;
import com.knockchat.hibernate.model.types.LongTimestampType;
import com.knockchat.hibernate.model.types.PointType;
import com.knockchat.hibernate.model.types.ShortListType;
import com.knockchat.hibernate.model.types.StringListType;
import com.knockchat.hibernate.model.types.StringSetType;
import com.knockchat.hibernate.model.types.TBaseLazyType;
import com.knockchat.hibernate.model.types.TEnumTypeFactory;
import com.knockchat.utils.thrift.TBaseLazy;
import com.knockchat.utils.thrift.Utils;

public class Column {
	
	private static final Logger log = LoggerFactory.getLogger(Column.class);
	
	
	protected final Table table;
	
    protected String columnName;
    protected int jdbcType;
    protected String  columnType;
    protected boolean nullable;
    protected Integer length;
    protected Integer scale;
    protected Class javaClass;
    protected String propertyName;
    protected boolean isAutoincrement;
    
    protected String hibernateType;
    protected String customRead;
    protected String customWrite;
    
    public Column(Table table){
    	this.table = table;
    }

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
        return this.propertyName!=null && this.javaClass != null && this.hibernateType !=null;
    }

    public static class ColumnExtractor implements ResultSetExtractor<List<Column>> {
    	
    	private final Table table;

        public ColumnExtractor(Table table) {
			super();
			this.table = table;
		}

		@Override
        public List<Column> extractData(ResultSet rs) throws SQLException, DataAccessException {
            List<Column> columns = new ArrayList<>();
            while(rs.next()){
                Column column = new Column(table);
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

    public boolean setHibernateType() {

    	final String logFmt = "{}.{}({}/{})  <-> {}.{}({})";
    	final Object[] logArgs = new Object[]{table.getTableName(), columnName, jdbcType, columnType, table.javaClass.getSimpleName(), propertyName, javaClass !=null ? javaClass.getSimpleName(): "null"};

    	if (propertyName == null){
    		log.trace("Skip not existing " + logFmt, logArgs);
    		return false;
    	}else if(javaClass == null) {
    		log.warn(logFmt, logArgs);
    		return false;
    	}else{
    		log.debug(logFmt, logArgs);
    	}
    	

    	if (javaClass == Long.class || javaClass == long.class) {            
    		switch(jdbcType){
    			case Types.TIMESTAMP:
    				hibernateType = LongTimestampType.class.getCanonicalName();
    				break;

    			case Types.DATE:
    				hibernateType = LongDateType.class.getCanonicalName();
    				break;

    			case Types.OTHER:
    				if (columnType.equalsIgnoreCase("interval")){
    					hibernateType = LongIntervalType.class.getCanonicalName();
    					customRead = "extract(epoch from " + columnName + ")";
    				}
    				break;
    			default:
    				hibernateType = LongType.INSTANCE.getName();
    		}            
    	} else if (javaClass == Short.class || javaClass == short.class) {
    		    		
    		hibernateType = ShortType.INSTANCE.getName();
    		
    	} else if (javaClass == Integer.class || javaClass == int.class ) {
    		
    		hibernateType = IntegerType.INSTANCE.getName();
    		
    	} else if (javaClass == Byte.class || javaClass == byte.class ) {
    		
    		hibernateType = ByteType.INSTANCE.getName();
    		
    	} else if (javaClass == Float.class || javaClass == float.class) {
    		
    		hibernateType = FloatType.INSTANCE.getName();
    		
    	} else if (javaClass == Double.class || javaClass == double.class ) {
    		
    		hibernateType = DoubleType.INSTANCE.getName();
    		
    	} else if (javaClass == Character.class || javaClass == char.class) {
    		
    		hibernateType = CharacterType.INSTANCE.getName();
    		
    	} else if (javaClass == String.class) {
    		
    		hibernateType = StringType.INSTANCE.getName();
    		
    		switch(jdbcType){
    			case Types.TIMESTAMP:
    				
    				customRead = columnName+"::text";
    				customWrite = "?::timestamp";
    				break;
    				
    			case Types.OTHER:
    				
    				if (columnType.equals("geography")) {
    					customRead = columnName + "::text";
    					customWrite = "?::geography";
    				} else if (columnType.equals("inet")){
    					customRead = columnName + "::text";
    					customWrite = "?::inet";
    				}
    				break;
    		}
    		    		
    	} else if (java.util.Date.class.isAssignableFrom(javaClass)) {
    		
    		switch (jdbcType) {
    			case Types.DATE:
    				hibernateType = DateType.INSTANCE.getName();
    				break;
    			case Types.TIME:
    				hibernateType = TimeType.INSTANCE.getName();
    				break;
    			case Types.TIMESTAMP:
    				hibernateType = TimestampType.INSTANCE.getName();
    				break;
    		}
    		
    	} else if (javaClass == Boolean.class || javaClass == boolean.class) {
    		
    		if(jdbcType == Types.BIT || jdbcType == Types.BOOLEAN) {
    			hibernateType = BooleanType.INSTANCE.getName();
    		} else if(jdbcType == Types.NUMERIC || jdbcType == Types.DECIMAL || jdbcType == Types.INTEGER || jdbcType == Types.SMALLINT || jdbcType == Types.TINYINT || jdbcType == Types.BIGINT) {
    			hibernateType = NumericBooleanType.INSTANCE.getName();
    		}
    		
    	} else if (javaClass == BigDecimal.class) {
    		
    		hibernateType = BigDecimalType.INSTANCE.getName();
    	
    	} else if (javaClass == BigInteger.class) {
    		
    		hibernateType = BigIntegerType.INSTANCE.getName();
    		
    	} else if (javaClass == byte[].class) {
    		
    		hibernateType = com.knockchat.hibernate.model.types.BinaryType.class.getCanonicalName();
    		
    	}else if (java.util.List.class.equals(javaClass)){
    		
    		if (columnType.contains("float") || columnType.contains("float8")) {
    			
    			hibernateType = DoubleListType.class.getCanonicalName();
    			
    		}else if (columnType.contains("_int8")) {

    			hibernateType = LongListType.class.getCanonicalName();

    		}else if (columnType.contains("_int4")){

    			hibernateType = IntegerListType.class.getCanonicalName();

    		}else if (columnType.contains("_short")){

    			hibernateType = ShortListType.class.getCanonicalName();

    		}else if (columnType.contains("_varchar") || columnType.contains("_text")) {

    			hibernateType = StringListType.class.getCanonicalName();
    		}
    		
    	}else if (java.util.Set.class.equals(javaClass)){
    		
    		if (columnType.contains("_varchar") || columnType.contains("_text")) {
    			hibernateType = StringSetType.class.getCanonicalName();
    		}
    		
    	}else if (Map.class.equals(javaClass)){
    		
    		if (columnType.contains("hstore")){
    			hibernateType = com.knockchat.hibernate.model.types.HstoreType.class.getCanonicalName();
    			customRead = columnName+"::hstore";
    			customWrite = "?::hstore";
    		}
    		
    	}else if (TLongLongHashMap.class.equals(javaClass)){
    		
    		if (columnType.contains("hstore")){
    			hibernateType = LongLongHstoreType.class.getCanonicalName();
    			customRead = columnName+"::hstore";
    			customWrite = "?::hstore";
    		}
    		
    	}else if (TEnum.class.isAssignableFrom(javaClass)){
    		
    		hibernateType = TEnumTypeFactory.create(javaClass).getCanonicalName();

    	}else if (jdbcType == Types.DATE && com.knockchat.hibernate.model.types.DateType.isCompatible(javaClass)) {
    		
    		hibernateType = CustomTypeFactory.create(javaClass, com.knockchat.hibernate.model.types.DateType.class).getCanonicalName();

    	}else if (jdbcType == Types.OTHER && columnType.contains("box2d") && BoxType.isCompatible(javaClass)) {
    		
    		hibernateType = CustomTypeFactory.create(javaClass, BoxType.class).getCanonicalName();

    	}else if (jdbcType == Types.OTHER && columnType.contains("geometry") && PointType.isCompatible(javaClass)) {
    		
    		hibernateType = CustomTypeFactory.create(javaClass, PointType.class).getCanonicalName();
    		customRead = "st_astext(" +columnName+ ")";
    		customWrite = "?::geometry";
    	
    	}else if (jdbcType == Types.OTHER && columnType.contains("jsonb")){
    		
    		hibernateType = CustomTypeFactory.create(javaClass, JsonType.class).getCanonicalName();        	
    	}else if (jdbcType == Types.BINARY && TBaseLazy.class.isAssignableFrom(javaClass)){
    		
    		if (!TBase.class.isAssignableFrom(javaClass)){
    			log.error("Error mapping " + logFmt, logArgs);
    			throw new RuntimeException("TBaseLazy must implement TBase");
    		}
    		
    		final Class rootThriftCls = Utils.getRootThriftClass(javaClass).first;
    		if (rootThriftCls != javaClass){
    			log.error("Error mapping " + logFmt, logArgs);
    			throw new RuntimeException("TBaseLazy field must be root thrift class");
    		}
    		
    		hibernateType = CustomTypeFactory.create(javaClass, TBaseLazyType.class).getCanonicalName();
    	}
    	
    	
    	if (hibernateType == null){
    		log.error("Unknown mapping " + logFmt, logArgs);
    		return false;
    	}

    	return true;
    }
    
    public String toHbmXmlVersion() {
    	if (!this.isValid()) return null;
    	
    	final StringBuilder sb = new StringBuilder(); 

    	sb.append(String.format("<version name=\"%s\" column=\"%s\" type=\"%s\" access=\"%s\"/>\n",
    			propertyName, columnName, hibernateType, ThriftPropertyAccessStrategy.class.getCanonicalName()));
    	    	
    	return sb.toString();
    }	

    
    public String toHbmXmlPk() {
    	if (!this.isValid()) return null;
    	
    	final StringBuilder sb = new StringBuilder(); 

    	sb.append(String.format("<id name=\"%s\" type=\"%s\" access=\"%s\">",
    			propertyName, hibernateType, ThriftPropertyAccessStrategy.class.getCanonicalName()));
    	
    	String column = String.format("<column name=\"%s\" not-null=\"true\" sql-type=\"%s\" ", columnName, columnType);

    	if (customRead !=null)
    		column += String.format("read=\"%s\" ", customRead);

    	if (customWrite !=null)
    		column += String.format("write=\"%s\" ", customWrite);
    	
    	column += "/>";
    	    	
    	final String generator= String.format("<generator class=\"%s\"/>", (isAutoincrement() || table.isView()) ? "identity" : "assigned");
    	
    	sb.append("\n\t");
    	sb.append(column);
    	sb.append("\n\t");
    	sb.append(generator);    	
    	sb.append("\n</id>\n");
    	
    	return sb.toString();
    }	
    
    public String toHbmXml() {
    	if (!this.isValid()) return null;
    	
    	final StringBuilder sb = new StringBuilder(); 

    	sb.append(String.format("<property name=\"%s\" not-null=\"%s\" type=\"%s\" lazy=\"false\" optimistic-lock=\"true\" update=\"true\" insert=\"true\" access=\"%s\">",
    			propertyName, Boolean.toString(!nullable), hibernateType, ThriftPropertyAccessStrategy.class.getCanonicalName()));
    	
    	String column = String.format("<column name=\"%s\" not-null=\"%s\" sql-type=\"%s\" ", columnName, Boolean.toString(!nullable), columnType);

    	if (customRead !=null)
    		column += String.format("read=\"%s\" ", customRead);

    	if (customWrite !=null)
    		column += String.format("write=\"%s\" ", customWrite);
    	
    	column += "/>";
    	    	
    	sb.append("\n\t");
    	sb.append(column);
    	sb.append("\n</property>\n");
    	
    	return sb.toString();
    }	
}

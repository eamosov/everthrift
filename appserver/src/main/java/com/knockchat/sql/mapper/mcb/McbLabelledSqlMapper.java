package com.knockchat.sql.mapper.mcb;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.knockchat.sql.pgarray.Jdbc4Array;
import com.knockchat.utils.meta.MetaClass;
import com.knockchat.utils.meta.MetaClasses;
import com.knockchat.utils.meta.MetaProperty;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class McbLabelledSqlMapper<ObjectType> extends AbstractMcbSqlMapper<ObjectType> {

	/**
	 * Логгер
	 */
	private static final Logger log = LoggerFactory.getLogger( McbLabelledSqlMapper.class );
	
	private static class MetaField{
		final Map<String, MetaField> processedProperties = new HashMap<String,MetaField>();
		final Map<String, MetaField> labelledProperties = new HashMap<String,MetaField>();
		final MetaProperty property;
		final MetaClass metaClass;
		final FieldMetaData fmd;
		final TFieldIdEnum fieldId;
				
		boolean extended = false;
		
		MetaField(MetaClass metaClass){
			this(null, metaClass, null, null);
		}
		
		MetaField(MetaProperty property, MetaClass metaClass, FieldMetaData fmd, TFieldIdEnum fieldId){
			this.property = property;
			this.metaClass = metaClass;
			this.fmd = fmd;
			this.fieldId = fieldId;
			
			if (this.metaClass == null)
				extended = true;
		}
		
		void set(Object object, Object columnValue){
			if (fieldId !=null){
				((TBase)object).setFieldValue(fieldId, columnValue);
			}else{						
				property.set( object, columnValue );
			}			
		}
				
		synchronized MetaField getLabelledProperty(String columnLabel, String columnClass){
			if (!extended)
				extend();
			
			MetaField prop = labelledProperties.get( columnLabel );

			if ( prop != null )
				return prop;

			prop = processedProperties.get( processName( columnLabel ) );

			if ( prop != null )
				labelledProperties.put( columnLabel, prop );

			return prop;		
		}
		
		@SuppressWarnings("unchecked")
		private static Method getFindFieldIdByNameMethod(MetaClass metaClass){
			final Class<?>[] subClasses = metaClass.getObjectClass().getClasses();
			Class<? extends TFieldIdEnum> _fields = null;
			
			for (Class<?> s: subClasses){
				if (s.getSimpleName().equals("_Fields")){
					_fields = (Class<? extends TFieldIdEnum>) s;
				}
			}
			
			if (_fields == null)
				return null;
			
			try {
				return  _fields.getMethod("findByName", String.class);
			} catch (Exception e) {
			}
			
			return null;
		}
		
		@SuppressWarnings("unchecked")
		private static Map<? extends TFieldIdEnum, FieldMetaData> getMetaDataMap(MetaClass metaClass){			
			Field fMetaDataMap;
			try {
				fMetaDataMap = metaClass.getObjectClass().getField("metaDataMap");
				return (Map<? extends TFieldIdEnum, FieldMetaData>) fMetaDataMap.get(null);
			} catch (Exception e) {
				
			}
			return null;		
		}
		
		synchronized void extend(){
			
			if (metaClass==null){
				extended = true;
				return;
			}
			
			final Method findFieldIdByName = getFindFieldIdByNameMethod(metaClass);
			
			final Map<? extends TFieldIdEnum, FieldMetaData> metaDataMap = findFieldIdByName == null ? null : getMetaDataMap(metaClass);
						
			for ( MetaProperty prop : metaClass.getProperties() ) {
				final String processedName = processName( prop.getName() );
			
				final MetaField oldProp = processedProperties.get( processedName );

				if ( oldProp != null )
					log.warn( "Processed property name {} conflict in class {}, {} and {}", new Object[]{ processedName, metaClass.getName(), oldProp.property.getName(), prop.getName() } );
				
				
				TFieldIdEnum f= null;
				FieldMetaData fmd = null;

				if (metaDataMap !=null){
									
					try {
						f = (TFieldIdEnum) findFieldIdByName.invoke(null, prop.getName());
						if (f!=null){
							fmd = metaDataMap.get(f);
							
							if (fmd==null){
								log.error("Couldn't find FieldMetaData for field '{}'", prop.getName());
							}
							
							log.trace("Processed thrift property. original name: {}, processedName:{}", prop.getName(), processedName);
						}
					} catch (Exception e) {
						log.error("Exception while find field '" + prop.getName() +  "'metadata", e);
					}
										
				}
				
				final MetaField childField = new MetaField(prop, (prop.getType()!=null && !prop.getType().isPrimitive() && !prop.getType().isArray()) ? MetaClasses.get(prop.getType()): null, fmd, f);

				processedProperties.put( processedName,  childField);
			}				
						
			extended = true;
		}				
	}

	private final MetaField properties;
		
	private ResultSetMetaData resultSetMetaData;
	//"dd MMM YYYY HH:mm:ss zzz"
	
	private FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd");
	private FastDateFormat timeFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ssZ");
	
	private synchronized ResultSetMetaData getMetaData(ResultSet rs) throws SQLException{
		if (resultSetMetaData !=null)
			return resultSetMetaData;
		
		resultSetMetaData = rs.getMetaData();
		return resultSetMetaData;
	}
	
	/**
	 * Класс заправшивает ResultSetMetaData только 1 раз, поэтому нужен новый instance для каждого нового запроса, отличающегося 
	 * 
	 * @param objectClass
	 * @param query
	 */
	public McbLabelledSqlMapper( Class<ObjectType> objectClass, String query ) {
		super( objectClass );
		properties = new MetaField(metaClass);
		properties.extend();		
	}

	@Override
	public void loadCurrent(ResultSet rs, Object object ) throws SQLException {
		final ResultSetMetaData metaData = getMetaData(rs);

		for ( int i = 0; i < metaData.getColumnCount(); ++ i ) {
			
			final long startMicros = System.nanoTime() / 1000;
			try{
				loadCurrent(metaData, i+1, properties, metaData.getColumnLabel( i+1 ), metaData.getColumnClassName( i+1 ), object, rs.getObject( i+1 ));
			}finally{
				final long endMicros  = System.nanoTime() / 1000;
				if (log.isTraceEnabled())
					log.trace("laodCurrent, field={}, objectClass={} in {} mcs", new Object[]{i, object.getClass(), (endMicros - startMicros)});
			}
		}
	}
		
	private Object castToThrift(FieldValueMetaData thriftType, String s) throws SQLException{

            if (s == null)
                    return null;

            switch (thriftType.type){
            case TType.BOOL:
                    return Boolean.parseBoolean(s);
            case TType.BYTE:
                    return Byte.parseByte(s);
            case TType.DOUBLE:
                    return Double.parseDouble(s);
            case TType.I16:
                    return Short.parseShort(s);
            case TType.I32:
                    return Integer.parseInt(s);
            case TType.I64:
                    return Long.parseLong(s);
            case TType.STRING:
                    return s;
            case TType.STRUCT:
            		
				
				try {
					final StructMetaData smd = (StructMetaData)thriftType;
					final TBase struct = smd.structClass.newInstance();
            		final byte[] bytes = s.getBytes("UTF-8");
            		final TMemoryBuffer tr = new TMemoryBuffer(bytes.length);
           			tr.write(bytes);
           			final TJSONProtocol pr = new TJSONProtocol(tr); 
            		struct.read(pr);
            		return struct;						
				} catch (Exception e) {
					log.error("cound't parse JSON object", e);
					throw new SQLException("cound't parse JSON object");
				}
            		
            default:
                    log.error("thrift type {} not supported", thriftType);
                    throw new SQLException("type not supported");
            }
    }
    
	private Object castSqlObject(Class javaClass, Number s) throws SQLException{
		if (s == null)
			return null;
		
		if (javaClass.isAssignableFrom(Byte.TYPE) || javaClass.isAssignableFrom(Byte.class)){
			return s.byteValue();
		}else if (javaClass.isAssignableFrom(Short.TYPE) || javaClass.isAssignableFrom(Short.class)){
			return s.shortValue();
		}else if (javaClass.isAssignableFrom(Integer.TYPE) || javaClass.isAssignableFrom(Integer.class)){
			return s.intValue();
		}else if (javaClass.isAssignableFrom(Long.TYPE) || javaClass.isAssignableFrom(Long.class)){
			return s.longValue();
		}else if (javaClass.isAssignableFrom(Double.TYPE) || javaClass.isAssignableFrom(Double.class)){
			return s.doubleValue();
		}else if (javaClass.isAssignableFrom(String.class)){
			return s.toString();
		}else{
			return s;
		}
	}
	
    @SuppressWarnings({ "unchecked", "rawtypes" })
	private Object castSqlObject(ResultSetMetaData metaData, int columnNumber, MetaField property, Object columnValue) throws SQLException{
    	
    	if (columnValue == null)
    		return null;
    	
    	final Class javaClass = property.property.getType();
    	
    	if (javaClass.isAssignableFrom(columnValue.getClass()))
    		return columnValue;
    	
    	if (javaClass.isAssignableFrom(List.class) && (columnValue instanceof Array)){
			final List<?> list = sqlArrayToList((Array) columnValue);
			return list;
		}
    	
    	if (javaClass.isAssignableFrom(List.class) && (columnValue instanceof String)){

    		try{
    			return sqlArrayToList((Array)new Jdbc4Array( Types.INTEGER, (String)columnValue));
    		}catch (SQLException e){
    			return sqlArrayToList((Array)new Jdbc4Array( Types.CHAR, (String)columnValue));
    		}												
    	}
    	
    	if ((javaClass.isAssignableFrom(Boolean.TYPE) || javaClass.isAssignableFrom(Boolean.class)) && (columnValue instanceof String)){
    		return Boolean.valueOf((String)columnValue);
    	}
    	
    	if (javaClass.isAssignableFrom(String.class) && (columnValue instanceof java.sql.Timestamp)){
    		return timeFormat.format((java.sql.Timestamp)columnValue);
    		//return ((java.sql.Timestamp)columnValue).toGMTString();
    	}

    	if (javaClass.isAssignableFrom(String.class) && (columnValue instanceof java.sql.Date)){
    		return dateFormat.format((java.sql.Date)columnValue);
    		//return ((java.sql.Date)columnValue).toGMTString();
    	}
    	
    	if (javaClass.isAssignableFrom(String.class) && metaData.getColumnTypeName(columnNumber).equalsIgnoreCase("hstore")){
    		return ((PGobject)columnValue).getValue();
    	}

    	if (javaClass.isAssignableFrom(String.class) && metaData.getColumnTypeName(columnNumber).equalsIgnoreCase("inet")){
    		return ((PGobject)columnValue).getValue();
    	}
    	
    	if (javaClass.isAssignableFrom(Map.class) && metaData.getColumnTypeName(columnNumber).equalsIgnoreCase("hstore")){
			final Map<String,String> pgValue = (Map)columnValue;
			
			if (pgValue.isEmpty()){
				return new HashMap();
			}else if (property.fmd !=null && property.fmd.valueMetaData.getClass().isAssignableFrom(MapMetaData.class)){

				final MapMetaData mmd = (MapMetaData)property.fmd.valueMetaData;
				
				final Map<Object,Object> hstore = new HashMap<Object, Object>();

				for (Entry<String, String> e: pgValue.entrySet()){
					if (mmd !=null)
						hstore.put(castToThrift(mmd.keyMetaData, e.getKey()), castToThrift(mmd.valueMetaData, e.getValue()));
				}
				
				return hstore;
			}else{
				return columnValue;
			}
    	}
    	
    	if (columnValue instanceof Number){
    		return castSqlObject(javaClass, (Number)columnValue);
    	}
    	
    	return columnValue;   	
    }

	private void loadCurrent(ResultSetMetaData metaData, int columnNumber, MetaField properties, String columnLabel, String columnClass, Object object, Object columnValue ) throws SQLException {
			MetaField property = properties.getLabelledProperty(columnLabel ,  columnClass);

			if ( property != null ){
				final Class<?> propCls = property.property.getType();
				
				if (propCls == null)
					throw new SQLException("propCls is NULL");
					
				try{
					
					property.set( object, castSqlObject(metaData, columnNumber, property, columnValue) );
					
				}catch (java.lang.ClassCastException e){
					log.error("error setting field '" +  columnLabel + "'", e);
					
					log.error("propCls='{}'", propCls.getName());
					log.error("columnValue='{}'", columnValue.getClass().getName());

					log.error("columnType={}", metaData.getColumnType(columnNumber));
					log.error("columnTypeName={}", metaData.getColumnTypeName(columnNumber));
					log.error("columnName={}", metaData.getColumnName(columnNumber));
					
					throw e;
				}
				return;
			}
			
			int dollar = columnLabel.indexOf('$');
			if (dollar < 0){
				log.debug("coudn't map column {}", columnLabel);
				return;
			}
			
			final String superFieldLabel = columnLabel.substring(0, dollar);
			final String subFieldLabel = columnLabel.substring(dollar+1);
			
			property = properties.getLabelledProperty(superFieldLabel ,  columnClass);
			if (property == null){
				log.warn("coudn't map column {}", columnLabel);
				return;
			}
			
			Object subObject = property.property.get(object);
				try {
					if (subObject == null){
						subObject = property.property.getType().newInstance();
						property.set(object, subObject);
					}
					loadCurrent(metaData, columnNumber, property, subFieldLabel, columnClass, subObject, columnValue);
				} catch (InstantiationException e) {
					log.error("InstantiationException", e);
				} catch (IllegalAccessException e) {
					log.error("IllegalAccessException", e);
				}
	}

	@SuppressWarnings("unchecked")
	private static <T> List<T> sqlArrayToList(Array sqlArray) throws SQLException{
		if (sqlArray == null)
			return null;
		
		List<T> list;
		
		T arr[] = (T [])sqlArray.getArray();
		list = new ArrayList<T>(arr.length);
		for (T i:arr){
			list.add(i);
		}
		return list;
	}

}

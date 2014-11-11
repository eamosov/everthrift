package com.knockchat.sql.mapper;

import java.lang.reflect.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.knockchat.sql.objects.PostQueryIF;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public abstract class AbstractSqlMapper<ObjectType> implements SqlMapper<ObjectType> {
	
	private static final Logger log = LoggerFactory.getLogger(AbstractSqlMapper.class);
	
	protected final Class<ObjectType> objectClass;
	
	public AbstractSqlMapper(Class<ObjectType> objectClass) {
		this.objectClass = objectClass;
	}

	@Override
	public final Class<ObjectType> getObjectClass() {
		return objectClass;
	}
	
	protected ObjectType loadCurrent( ResultSet rs) throws SQLException{
		ObjectType obj;
		try {
			obj = objectClass.newInstance();
		} catch ( Throwable e ) {
			throw new SQLException( "Error instantiating " + objectClass.getName(), e );
		}
		loadCurrent(rs, obj );
		
		if (obj instanceof PostQueryIF){
			((PostQueryIF)obj).postQuery();
		}
		return obj;
	}
	
	private int loadArray( ResultSet rs, ObjectType[] array, int count ) throws SQLException {
		int index = 0; // Начальный индекс
		
		while ( index < count && rs.next() ) {// Пока загрузили меньше заданного количества
			array[ index ] = loadCurrent(rs ); // Загружаем следующий объект
			++ index; // Увеличиваем индекс
		}
		
		return index;
	}
	
	private int loadCollection( ResultSet rs, Collection<? super ObjectType> collection ) throws SQLException {
		int index = 0; // Начальный индекс
		
		while ( rs.next() ) { // Пока загрузили меньше заданного количества
			collection.add( loadCurrent(rs ) ); // Загружаем следующий объект
			++ index; // Увеличиваем индекс
		}
		
		return index;
	}
	
	private int loadCollection( ResultSet rs, Collection<? super ObjectType> collection, int count ) throws SQLException {
		int index = 0; // Начальный индекс
		
		while ( index < count && rs.next() ) { // Пока загрузили меньше заданного количества
			collection.add(loadCurrent(rs ) ); // Загружаем следующий объект
			++ index; // Увеличиваем индекс
		}
		
		return index;
	}
	
	private Object transformKeyObject(Object k){
		return k instanceof Long ? k : (k instanceof Number ? ((Number)k).longValue(): k);
	}
	
	private int loadMap( ResultSet rs, int keyIndex, Map<Object, ObjectType> map) throws SQLException{
		int index = 0;
		
		while(rs.next()){
			map.put( transformKeyObject(rs.getObject(keyIndex)), loadCurrent(rs));
			++index;
		}
		return index;
	}
		
	private Map<Object, ObjectType> loadMap( ResultSet rs, int keyIndex)  throws SQLException {
		final Map<Object, ObjectType> map = new HashMap<Object, ObjectType>();
		loadMap(rs, keyIndex, map);
		return map;
	}
	
	private int loadMultiMap( ResultSet rs, int keyIndex, Multimap<Object, ObjectType> map) throws SQLException{
		int index = 0;
		
		while(rs.next()){
			map.put(transformKeyObject(rs.getObject(keyIndex)), loadCurrent(rs));
			++index;
		}
		return index;
	}
	
	private Multimap<Object, ObjectType> loadMultiMap( ResultSet rs, int keyIndex)  throws SQLException {
		final Multimap<Object, ObjectType> map = ArrayListMultimap.create();
		loadMultiMap(rs, keyIndex, map);
		return map;
	}

	private int loadArray( ResultSet rs, ObjectType[] array ) throws SQLException {
		return loadArray(rs, array, array.length );
	}

	private ObjectType[] loadArray( ResultSet rs ) throws SQLException {
		Collection<ObjectType> collection = new ArrayList<ObjectType>(); // Создаем новую коллекцию
		loadCollection(rs, collection ); // Загружаем данные
		return collection.toArray(createObjectArray( collection.size() ) ); // Преобразуем коллекцию в массив
	}

	private ObjectType[] loadArray( ResultSet rs, int count ) throws SQLException {
		Collection<ObjectType> collection = new ArrayList<ObjectType>();
		loadCollection(rs, collection, count ); // Загружаем данные
		return collection.toArray(createObjectArray( collection.size() ) ); // Преобразуем коллекцию в массив
	}

	private ArrayList<ObjectType> loadCollection( ResultSet rs ) throws SQLException {
		ArrayList<ObjectType> collection = new ArrayList<ObjectType>(); // Создаем новую коллекцию
		loadCollection(rs, collection ); // Загружаем данные
		return collection;
	}

	private ArrayList<ObjectType> loadCollection( ResultSet rs, int count ) throws SQLException {
		ArrayList<ObjectType> collection = new ArrayList<ObjectType>(); // Создаем новую коллекцию
		loadCollection(rs, collection, count ); // Загружаем данные
		return collection;
	}

	@Override
	public abstract void loadCurrent( ResultSet rs, ObjectType object ) throws SQLException;

	@SuppressWarnings("unchecked")
	private ObjectType[] createObjectArray( int size ) {
		return (ObjectType[]) Array.newInstance( objectClass, size );
	}
	
	@Override
	public ResultSetExtractor<ObjectType> firstRowExtractor(){
		return new ResultSetExtractor<ObjectType>(){

			@Override
			public ObjectType extractData(ResultSet rs) throws SQLException, DataAccessException {
				
				final long startMicros = System.nanoTime() / 1000;
				
				try{
					return rs.next() ? loadCurrent(rs) : null;
				}finally{
					final long endMicros = System.nanoTime() / 1000;
					log.trace("extractData() in {} mcs", (endMicros - startMicros));					
				}				
			}};
	}

	@Override
	public ResultSetExtractor<List<ObjectType>> listExtractor(){
		return new ResultSetExtractor<List<ObjectType>>(){

			@Override
			public List<ObjectType> extractData(ResultSet rs) throws SQLException, DataAccessException {
				final long startMicros = System.nanoTime() / 1000;
				
				try{
					return loadCollection(rs);
				}finally{
					final long endMicros = System.nanoTime() / 1000;
					log.trace("extractData() in {} mcs", (endMicros - startMicros));					
				}								
			}};
	}
	
	@Override
	public ResultSetExtractor<ObjectType[]> arrayExtractor(){
		return new ResultSetExtractor<ObjectType[]>(){

			@Override
			public ObjectType[] extractData(ResultSet rs) throws SQLException, DataAccessException {
				return loadArray(rs);
			}};		
	}
	
	@Override
	public ResultSetExtractor<Map<Object, ObjectType>> mapExtractor(final int keyIndex){
		return new ResultSetExtractor<Map<Object, ObjectType>>(){

			@Override
			public Map<Object, ObjectType> extractData(ResultSet rs) throws SQLException, DataAccessException {
				return loadMap(rs, keyIndex);
			}};		
	}
	
	@Override
	public ResultSetExtractor<Multimap<Object, ObjectType>> multiMapExtractor(final int keyIndex){
		return new ResultSetExtractor<Multimap<Object, ObjectType>>(){

			@Override
			public Multimap<Object, ObjectType> extractData(ResultSet rs) throws SQLException, DataAccessException {
				return loadMultiMap(rs, keyIndex);
			}};				
	}
	
	@Override
	public ObjectType mapRow(ResultSet rs, int rowNum) throws SQLException {
		
		ObjectType object;
		try {
			object = this.objectClass.newInstance();
		} catch (InstantiationException e) {
			throw new SQLException(e);
		} catch (IllegalAccessException e) {
			throw new SQLException(e);
		}
		loadCurrent(rs, object);
		return object;
	}	
}

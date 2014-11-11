package com.knockchat.sql.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

import com.google.common.collect.Multimap;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public interface SqlMapper<ObjectType>  extends RowMapper<ObjectType>{

	Class<ObjectType> getObjectClass();
	
	void loadCurrent( ResultSet rs, ObjectType object ) throws SQLException;
/*
	
	ObjectType loadCurrent( ResultSet rs) throws SQLException;

	int loadCollection( ResultSet rs, Collection<? super ObjectType> collection ) throws SQLException;
	List<ObjectType> loadCollection( ResultSet rs ) throws SQLException;
	
	int loadMap( ResultSet rs, int keyIndex, Map<Object, ObjectType> map) throws SQLException;
	Map<Object, ObjectType> loadMap( ResultSet rs,  int keyIndex)  throws SQLException;

	int loadMultiMap( ResultSet rs, int keyIndex, MultiMap<Object, ObjectType> map) throws SQLException;
	MultiMap<Object, ObjectType> loadMultiMap( ResultSet rs,  int keyIndex)  throws SQLException;
	
	int loadCollection( ResultSet rs, Collection<? super ObjectType> collection, int count ) throws SQLException;
	List<ObjectType> loadCollection( ResultSet rs, int count ) throws SQLException;

	int loadArray( ResultSet rs, ObjectType[] array ) throws SQLException;
	ObjectType[] loadArray( ResultSet rs ) throws SQLException;

	int loadArray( ResultSet rs, ObjectType[] array, int count ) throws SQLException;
	ObjectType[] loadArray( ResultSet rs, int count ) throws SQLException;
*/	
	public ResultSetExtractor<ObjectType> firstRowExtractor();
	public ResultSetExtractor<List<ObjectType>> listExtractor();
	public ResultSetExtractor<ObjectType[]> arrayExtractor();
	public ResultSetExtractor<Map<Object, ObjectType>> mapExtractor(int keyIndex);
	public ResultSetExtractor<Multimap<Object, ObjectType>> multiMapExtractor(int keyIndex);
}

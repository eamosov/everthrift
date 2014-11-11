package com.knockchat.sql.objects;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Multimap;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public interface QueryStatement<ObjectType> extends SqlStatement {

	ObjectType queryFirst( Object... values );
	ObjectType[] queryArray( Object... values );
	List<ObjectType> queryList( Object... values );
	
	/**
	 *   keyIndex = номер поля в resultSet начиная с 1
	 */
	Map<Object, ObjectType> queryMap( int keyIndex,  Object... values );
	<K> Map<K, ObjectType> queryMap(Function<ObjectType, K> keyExtractor, Iterable<K> keysList, Object ...values);
	Multimap<Object, ObjectType> queryMultiMap( int keyIndex,  Object... values );	
	Map<Object, List<ObjectType>> queryMultiMapAsKeys(int keyIndex, Iterable keysList);
	
	CursorableResult<ObjectType> queryCursor(Connection con, int fetchSize, Object ...values) throws SQLException;
	<K> Map<K, List<ObjectType>> queryMultiMap(Function<ObjectType, K> keyExtractor, Iterable<K> keysList, Object ...values);
}

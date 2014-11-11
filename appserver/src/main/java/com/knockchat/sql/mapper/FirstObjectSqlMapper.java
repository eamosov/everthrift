package com.knockchat.sql.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.dbutils.ResultSetHandler;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class FirstObjectSqlMapper<ObjectType> extends AbstractSqlMapper<ObjectType> implements ResultSetHandler {

	public static final FirstObjectSqlMapper<Object> INSTANCE = new FirstObjectSqlMapper<Object>( Object.class);

	private static final int COLUMN_INDEX = 1;

	public FirstObjectSqlMapper( Class<ObjectType> objectClass ) {
		super( objectClass );
	}

	@Override
	public void loadCurrent(ResultSet rs, Object object ) throws SQLException {
		throw new UnsupportedOperationException();
	}

//	@Override
//	public List<Object> getParams(Object object) throws SQLException {
//		throw new UnsupportedOperationException();
//	}

	@SuppressWarnings("unchecked")
	@Override
	public ObjectType loadCurrent(ResultSet rs ) throws SQLException {
		return (ObjectType) rs.getObject( COLUMN_INDEX );
	}

	@Override
	public Object handle( ResultSet rs ) throws SQLException {
		return rs.next() ? rs.getObject( COLUMN_INDEX ) : null;
	}

	@Override
	public ObjectType mapRow(ResultSet rs, int rowNum) throws SQLException {
		if (rowNum == COLUMN_INDEX)
			return (ObjectType)rs.getObject( COLUMN_INDEX );
		else
			return null;
	}

}

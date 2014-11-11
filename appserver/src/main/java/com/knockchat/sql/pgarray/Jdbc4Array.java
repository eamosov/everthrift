/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2008, PostgreSQL Global Development Group
*
* IDENTIFICATION
*   $PostgreSQL: pgjdbc/org/postgresql/jdbc4/Jdbc4Array.java,v 1.4 2008/01/08 06:56:30 jurka Exp $
*
*-------------------------------------------------------------------------
*/
package com.knockchat.sql.pgarray;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

public class Jdbc4Array extends AbstractJdbc2Array implements Array
{
	
	private static int getSqlType(Class cls) throws SQLException{
		
		if (cls.equals(Integer.class))
			return Types.INTEGER;
		else if (cls.equals(Long.class))
			return Types.BIGINT;
		else if (cls.equals(String.class))
			return Types.VARCHAR;
		
		//cls.i
		
		throw new SQLException("coudn't get SQL type from field of type " + cls.toString());
	}
	
    public Jdbc4Array(int oid, String fieldString) throws SQLException
    {
        super(oid, fieldString);
    }

    @Override
	public Object getArray(Map < String, Class < ? >> map) throws SQLException
    {
        return getArrayImpl(map);
    }

    @Override
	public Object getArray(long index, int count, Map < String, Class < ? >> map) throws SQLException
    {
        return getArrayImpl(index, count, map);
    }

	@Override
	public void free() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getBaseType() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getBaseTypeName() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getResultSet(Map<String, Class<?>> map)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getResultSet(long index, int count) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getResultSet(long index, int count,
			Map<String, Class<?>> map) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

}

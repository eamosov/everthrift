package com.knockchat.sql.mapper;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class PreparedStatementDecorator implements PreparedStatement {
	
	private PreparedStatement decorated;
	
	public PreparedStatementDecorator(PreparedStatement decorated){
		this.decorated = decorated;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return decorated.unwrap(iface);
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		return decorated.executeQuery(sql);
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		return decorated.executeQuery();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return decorated.isWrapperFor(iface);
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		return decorated.executeUpdate(sql);
	}

	@Override
	public int executeUpdate() throws SQLException {
		return decorated.executeUpdate();
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		decorated.setNull(parameterIndex, sqlType);
	}

	@Override
	public void close() throws SQLException {
		decorated.close();
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		return decorated.getMaxFieldSize();
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		decorated.setBoolean(parameterIndex, x);
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		decorated.setByte(parameterIndex, x);
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		decorated.setMaxFieldSize(max);
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		decorated.setShort(parameterIndex, x);
	}

	@Override
	public int getMaxRows() throws SQLException {
		return decorated.getMaxRows();
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		decorated.setInt(parameterIndex, x);
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		decorated.setMaxRows(max);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		decorated.setLong(parameterIndex, x);
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		decorated.setEscapeProcessing(enable);
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		decorated.setFloat(parameterIndex, x);
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		decorated.setDouble(parameterIndex, x);
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		return decorated.getQueryTimeout();
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		decorated.setQueryTimeout(seconds);
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x)
			throws SQLException {
		decorated.setBigDecimal(parameterIndex, x);
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		decorated.setString(parameterIndex, x);
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		decorated.setBytes(parameterIndex, x);
	}

	@Override
	public void cancel() throws SQLException {
		decorated.cancel();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return decorated.getWarnings();
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		decorated.setDate(parameterIndex, x);
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		decorated.setTime(parameterIndex, x);
	}

	@Override
	public void clearWarnings() throws SQLException {
		decorated.clearWarnings();
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		decorated.setCursorName(name);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x)
			throws SQLException {
		decorated.setTimestamp(parameterIndex, x);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		decorated.setAsciiStream(parameterIndex, x, length);
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		return decorated.execute(sql);
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		decorated.setUnicodeStream(parameterIndex, x, length);
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return decorated.getResultSet();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		decorated.setBinaryStream(parameterIndex, x, length);
	}

	@Override
	public int getUpdateCount() throws SQLException {
		return decorated.getUpdateCount();
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		return decorated.getMoreResults();
	}

	@Override
	public void clearParameters() throws SQLException {
		decorated.clearParameters();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType)
			throws SQLException {
		decorated.setObject(parameterIndex, x, targetSqlType);
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		decorated.setFetchDirection(direction);
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return decorated.getFetchDirection();
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		decorated.setObject(parameterIndex, x);
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		decorated.setFetchSize(rows);
	}

	@Override
	public int getFetchSize() throws SQLException {
		return decorated.getFetchSize();
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		return decorated.getResultSetConcurrency();
	}

	@Override
	public boolean execute() throws SQLException {
		return decorated.execute();
	}

	@Override
	public int getResultSetType() throws SQLException {
		return decorated.getResultSetType();
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		decorated.addBatch(sql);
	}

	@Override
	public void clearBatch() throws SQLException {
		decorated.clearBatch();
	}

	@Override
	public void addBatch() throws SQLException {
		decorated.addBatch();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		return decorated.executeBatch();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length)
			throws SQLException {
		decorated.setCharacterStream(parameterIndex, reader, length);
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		decorated.setRef(parameterIndex, x);
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		decorated.setBlob(parameterIndex, x);
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		decorated.setClob(parameterIndex, x);
	}

	@Override
	public Connection getConnection() throws SQLException {
		return decorated.getConnection();
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		decorated.setArray(parameterIndex, x);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return decorated.getMetaData();
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		return decorated.getMoreResults(current);
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal)
			throws SQLException {
		decorated.setDate(parameterIndex, x, cal);
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		return decorated.getGeneratedKeys();
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal)
			throws SQLException {
		decorated.setTime(parameterIndex, x, cal);
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException {
		return decorated.executeUpdate(sql, autoGeneratedKeys);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
			throws SQLException {
		decorated.setTimestamp(parameterIndex, x, cal);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName)
			throws SQLException {
		decorated.setNull(parameterIndex, sqlType, typeName);
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes)
			throws SQLException {
		return decorated.executeUpdate(sql, columnIndexes);
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		decorated.setURL(parameterIndex, x);
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames)
			throws SQLException {
		return decorated.executeUpdate(sql, columnNames);
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		return decorated.getParameterMetaData();
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		decorated.setRowId(parameterIndex, x);
	}

	@Override
	public void setNString(int parameterIndex, String value)
			throws SQLException {
		decorated.setNString(parameterIndex, value);
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException {
		return decorated.execute(sql, autoGeneratedKeys);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value,
			long length) throws SQLException {
		decorated.setNCharacterStream(parameterIndex, value, length);
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		decorated.setNClob(parameterIndex, value);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		decorated.setClob(parameterIndex, reader, length);
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		return decorated.execute(sql, columnIndexes);
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length)
			throws SQLException {
		decorated.setBlob(parameterIndex, inputStream, length);
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		decorated.setNClob(parameterIndex, reader, length);
	}

	@Override
	public boolean execute(String sql, String[] columnNames)
			throws SQLException {
		return decorated.execute(sql, columnNames);
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject)
			throws SQLException {
		decorated.setSQLXML(parameterIndex, xmlObject);
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType,
			int scaleOrLength) throws SQLException {
		decorated.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return decorated.getResultSetHoldability();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return decorated.isClosed();
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		decorated.setPoolable(poolable);
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return decorated.isPoolable();
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		decorated.closeOnCompletion();
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		decorated.setAsciiStream(parameterIndex, x, length);
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		return decorated.isCloseOnCompletion();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		decorated.setBinaryStream(parameterIndex, x, length);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader,
			long length) throws SQLException {
		decorated.setCharacterStream(parameterIndex, reader, length);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x)
			throws SQLException {
		decorated.setAsciiStream(parameterIndex, x);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x)
			throws SQLException {
		decorated.setBinaryStream(parameterIndex, x);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader)
			throws SQLException {
		decorated.setCharacterStream(parameterIndex, reader);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value)
			throws SQLException {
		decorated.setNCharacterStream(parameterIndex, value);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		decorated.setClob(parameterIndex, reader);
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream)
			throws SQLException {
		decorated.setBlob(parameterIndex, inputStream);
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		decorated.setNClob(parameterIndex, reader);
	}
	

}

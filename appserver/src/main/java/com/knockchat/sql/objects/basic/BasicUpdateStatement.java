package com.knockchat.sql.objects.basic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import com.knockchat.sql.SqlUtils;
import com.knockchat.sql.mapper.mcb.McbFixedOrderSqlMapper;
import com.knockchat.sql.mapper.mcb.McbSqlMapperFactory;
import com.knockchat.sql.objects.UpdateStatement;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class BasicUpdateStatement<ObjectType> extends AbstractBasicStatement<ObjectType> implements UpdateStatement<ObjectType> {

	private static final Logger log = LoggerFactory.getLogger( BasicUpdateStatement.class );

	private final int propertyCount;

	private final McbFixedOrderSqlMapper<ObjectType> sqlMapper;

	public BasicUpdateStatement( Class<ObjectType> objectClass, JdbcTemplate jdbcTemplate, String sql, String[] properties ) {
		super(jdbcTemplate, sql );
		this.sqlMapper = McbSqlMapperFactory.INSTANCE.getFixedOrderSqlMapper( objectClass, properties );
		this.propertyCount = properties.length;
	}

	private int update( Connection c, Throwable trace, ObjectType obj, Object... values ) throws SQLException {

		try{
			return executeAndBenchmark(c, trace, obj, values ); // Выполняем
		}catch (SQLException e){
			if (log.isDebugEnabled())
				log.debug( "Eroror executing update \"{}\" with values ({})", sql, getValues(obj, values));
			throw new SQLException(e.getMessage() + ", code=" + e.getErrorCode() + ", statement \"" + sql + "\", values (" + getValues(obj, values) + ")", e.getSQLState(), e.getErrorCode(), e);
		}
	}
	
	private int executeAndBenchmark( Connection c, Throwable trace, ObjectType obj, Object []values) throws SQLException {
		
		final PreparedStatement st = c.prepareStatement(sql);
		
		try{
			fillParams( st, obj, values );		

			final long startMicros = System.nanoTime() / 1000;
			final int rs= st.executeUpdate(); // Выполняем запрос
			final long endMicros = System.nanoTime() / 1000;
			
			final long deltaMcs = endMicros - startMicros;

			ts.update( deltaMcs );
					
//			if (log.isDebugEnabled()){
//				final PGStatement pgstmt = (org.postgresql.PGStatement)st;
//				if (pgstmt.isUseServerPrepare()){
//					log.warn( "Query '{}' with values '{}' executed in {} mcs as server prepared statement", new Object[]{sql, getValues(obj, values), deltaMcs} );
//				}
//			}else{
				if (deltaMcs > settings.warnLimitMcs){
					log.warn( "Query '{}' with values '{}' executed in {} mcs", new Object[]{sql, getValues(obj, values), deltaMcs} );
				}else if (deltaMcs > settings.infoLimitMcs){
					log.info( "Query '{}' with values '{}' executed in {} mcs", new Object[]{sql, getValues(obj, values), deltaMcs} );
				}else if (deltaMcs > settings.debugLimitMcs){
					log.debug( "Query '{}' with values '{}' executed in {} mcs", new Object[]{sql, getValues(obj, values), deltaMcs} );
				}				
//			}
			
			updateExecutionStats( deltaMcs );
			return rs;
		}finally{
			st.close();
		}
	}

	private void fillParams( PreparedStatement st, ObjectType obj, Object []values ) throws SQLException {
		if ( log.isDebugEnabled() )
			logQuery( obj, values );

		sqlMapper.fillParams( st, obj ); // Заполняем парметры запроса из объекта

		for ( int i = 0; i < values.length; ++i )
			st.setObject( propertyCount + 1 + i, SqlUtils.toSqlParam( values[i] ) ); // Заполняем параметры запроса из переданных // Заполняем параметры запроса
	}

	private String getValues(ObjectType obj, Object[] atts) throws SQLException{
		StringBuilder valuesString = new StringBuilder();

		List<Object> values = sqlMapper.getParams( obj );

		Collections.addAll( values, atts );

		if ( values.size() > 0 ) {
			valuesString.append( " " );
			valuesString.append( quote( SqlUtils.toSqlParam( values.get( 0 ) ) ) );
			for ( int i = 1; i < values.size(); ++i ) {
				valuesString.append( ", " );
				valuesString.append( quote( SqlUtils.toSqlParam( values.get( i ) ) ) );
			}
			valuesString.append( " " );
		}
		return valuesString.toString();
	}
	
	private void logQuery( ObjectType obj, Object[] atts ) throws SQLException {	
		log.debug( "Executing update \"{}\" with values ({})", sql, getValues(obj, atts) );
	}

	@Override
	public int update(final ObjectType obj, final Object... values){
		final Throwable trace = new Throwable();
		return jdbcTemplate.execute(new ConnectionCallback<Integer>(){

			@Override
			public Integer doInConnection(Connection con) throws SQLException, DataAccessException {
				return update(con, trace, obj, values);
			}});		
	}

	@Override
	public int update(final Throwable trace, final ObjectType obj, final Object... values) throws SQLException {
		
		return jdbcTemplate.execute(new ConnectionCallback<Integer>(){

			@Override
			public Integer doInConnection(Connection con) throws SQLException, DataAccessException {
				return update(con, trace, obj, values);
			}});		
	}

}

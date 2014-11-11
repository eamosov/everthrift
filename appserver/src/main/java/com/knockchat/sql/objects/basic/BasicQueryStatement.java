package com.knockchat.sql.objects.basic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.knockchat.sql.SqlUtils;
import com.knockchat.sql.mapper.FirstObjectSqlMapper;
import com.knockchat.sql.mapper.PreparedStatementDecorator;
import com.knockchat.sql.mapper.SqlMapper;
import com.knockchat.sql.mapper.mcb.McbSqlMapperFactory;
import com.knockchat.sql.objects.CursorableResult;
import com.knockchat.sql.objects.QueryStatement;
import com.knockchat.utils.ClassUtils;
import com.knockchat.utils.NullKeysMap;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class BasicQueryStatement<ObjectType> extends AbstractBasicStatement<ObjectType> implements QueryStatement<ObjectType>, PreparedStatementCreator {

	private static final Logger log = LoggerFactory.getLogger( BasicQueryStatement.class );
	
	private final SqlMapper<ObjectType> sqlMapper;

	public BasicQueryStatement( Class<ObjectType> objectClass, JdbcTemplate jdbcTemplate, String sql ) {
		super(jdbcTemplate, sql);

		if ( objectClass.isPrimitive() || ClassUtils.isBoxType( objectClass ) || objectClass.equals( String.class ) ) {
			this.sqlMapper = new FirstObjectSqlMapper<ObjectType>( objectClass );
		} else {
			this.sqlMapper = McbSqlMapperFactory.INSTANCE.getLabelledSqlMapper( objectClass, sql );
		}
	}
	
	@Override
	public ObjectType queryFirst(final Object... values ){		
		return executeAndBenchmark(sqlMapper.firstRowExtractor(), values);
	}
	
	@Override
	public List<ObjectType> queryList(Object... values ){
		return executeAndBenchmark(sqlMapper.listExtractor(), values);
	}
	
	@Override
	public ObjectType[] queryArray(Object... values ){
		return executeAndBenchmark(sqlMapper.arrayExtractor(), values);
	}
	
	@Override
	public Map<Object, ObjectType> queryMap(int keyIndex, Object... values) {
		return executeAndBenchmark(sqlMapper.mapExtractor(keyIndex), values);
	}
	
	@Override
	public Multimap<Object, ObjectType> queryMultiMap(int keyIndex, Object... values) {
		return executeAndBenchmark(sqlMapper.multiMapExtractor(keyIndex), values);
	}

	@Override
	public Map<Object, List<ObjectType>> queryMultiMapAsKeys(int keyIndex, Iterable keysList) {
		final Multimap<Object, ObjectType> mmap = queryMultiMap(keyIndex, keysList);
		final Map<Object, List<ObjectType>> ret = Maps.newHashMapWithExpectedSize(mmap.keySet().size());
		
		for (Entry<Object, Collection<ObjectType>> e : mmap.asMap().entrySet()){
			ret.put(e.getKey(),  Lists.newArrayList(e.getValue()));
		}
		
		for (Object k: keysList){
			final Object key = k instanceof Long ? k : (k instanceof Number ? ((Number)k).longValue(): k);
			if (!ret.containsKey(key))
				ret.put(key, Collections.<ObjectType>emptyList());
		}
		
		return ret;		
	}	

	@Override
	public <K> Map<K, List<ObjectType>> queryMultiMap(Function<ObjectType, K> keyExtractor, Iterable<K> keysList, Object ... values) {

		final List<ObjectType> rs = queryList(values);
		final NullKeysMap<K, ObjectType> ret = new NullKeysMap<K, ObjectType>(keysList);
		
		for (ObjectType r: rs){
			ret.put(keyExtractor.apply(r), r);
		}
		
		return (Map)ret;		
	}	

	@Override
	public <K> Map<K, ObjectType> queryMap(Function<ObjectType, K> keyExtractor, Iterable<K> keysList, Object ... values) {

		final List<ObjectType> rs = queryList(values);
		final Map<K, ObjectType> ret = new HashMap<K, ObjectType>();
				
		for (ObjectType r: rs){
			ret.put(keyExtractor.apply(r), r);
		}
		
		for (K k : keysList)
			if (!ret.containsKey(k))
				ret.put(k, null);
				
		return (Map)ret;		
	}	
	
	@Override
	public CursorableResult<ObjectType> queryCursor(Connection con, int fetchSize, Object ...values) throws SQLException{
		
		if (con.getAutoCommit() == true)
			throw new SQLException("cursors require no autoCommit");
		
		final PreparedStatement ps = this.createPreparedStatement(con);
		
		this.getSetter(values).setValues(ps);
		ps.setFetchSize(fetchSize);
		final ResultSet rs = ps.executeQuery();
		
		return new CursorableResult<ObjectType>(){

			@Override
			public Iterator<ObjectType> iterator() {
				return new Iterator<ObjectType>(){

					private boolean needNext = true;
					@Override
					public boolean hasNext() {
						try {
							final boolean hasNext =  rs.next() != false;
							needNext = false;
							return hasNext;
						} catch (SQLException e) {
							throw new UncategorizedSQLException("queryCursor.hasNext", sql, e);
						}
					}

					@Override
					public ObjectType next() {
						try {
							if (needNext)
								rs.next();
							needNext = true;
							return sqlMapper.mapRow(rs, rs.getRow());
						} catch (SQLException e) {
							throw new UncategorizedSQLException("queryCursor.next", sql, e);
						}
						
					}

					@Override
					public void remove() {						
						throw new UnsupportedOperationException();
					}};
			}

			@Override
			public void close() {
				try {
					rs.close();
				} catch (SQLException e) {
				}
				
				try {
					ps.close();
				} catch (SQLException e) {
				}				
			}};
	}
	
	private <T> T executeAndBenchmark( ResultSetExtractor<T> rse, Object... values ) {

		final long startMicros = System.nanoTime() / 1000;

		try{			
			return jdbcTemplate.query(this, getSetter(values), rse);					
		}finally{
			final long endMicros = System.nanoTime() / 1000;
			final long deltaMcs = endMicros - startMicros;

			ts.update( deltaMcs );
			
			if (deltaMcs > settings.warnLimitMcs){
				log.warn( "Query '{}' with values '{}' executed in {} mcs", new Object[]{sql, getValues(values), deltaMcs} );
			}else if (deltaMcs > settings.infoLimitMcs){
				log.info( "Query '{}' with values '{}' executed in {} mcs", new Object[]{sql, getValues(values), deltaMcs} );
			}else if (deltaMcs > settings.debugLimitMcs){
				log.debug( "Query '{}' with values '{}' executed in {} mcs", new Object[]{sql, getValues(values), deltaMcs} );
			}							
			
			updateExecutionStats( deltaMcs );
		}
	}

	private String getValues( Object[] values ) {
		StringBuilder valuesString = new StringBuilder();
		if ( values.length > 0 ) {
			valuesString.append( " " );
			valuesString.append( quote( SqlUtils.toSqlParam( values[0] ) ) );
			for ( int i = 1; i < values.length; ++i ) {
				valuesString.append( ", " );
				valuesString.append( quote( SqlUtils.toSqlParam( values[i] ) ) );
			}
			valuesString.append( " " );
		}

		return valuesString.toString();
	}

	@Override
	public PreparedStatement createPreparedStatement(Connection con) throws SQLException {

		final PreparedStatement st = new PreparedStatementDecorator(con.prepareStatement(sql)){
			@Override
			public ResultSet executeQuery() throws SQLException {
				final long startMicros = System.nanoTime() / 1000;
				try{
					return super.executeQuery();
				}finally{
					final long endMicros = System.nanoTime() / 1000;
					log.trace("Executing statement in {} mcs", (endMicros - startMicros));
				}
			}
		}; // Подготавливаем SQL
		return st;
	}
	
	private PreparedStatementSetter getSetter(final Object ... values){
		return new PreparedStatementSetter(){

			@Override
			public void setValues(PreparedStatement ps) throws SQLException {
				
				final long startMicros = System.nanoTime() / 1000;
				
				try{
					for ( int i = 0; i < values.length; ++i )
						ps.setObject( i + 1, SqlUtils.toSqlParam( values[i] ) ); // Устанавливаем все параметры запроса					
				}finally{
					final long endMicros = System.nanoTime() / 1000;
					log.trace("setValues() in {} mcs", (endMicros - startMicros));					
				}				
			}};		
	}

}

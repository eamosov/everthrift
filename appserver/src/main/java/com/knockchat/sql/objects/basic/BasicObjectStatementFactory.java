package com.knockchat.sql.objects.basic;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.knockchat.sql.objects.ObjectStatementFactory;
import com.knockchat.sql.objects.QueryStatement;
import com.knockchat.sql.objects.UpdateStatement;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
@Component
public class BasicObjectStatementFactory implements ObjectStatementFactory {
	
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
    @Qualifier("dataSource")
	public void setDataSource(DataSource ds){
		jdbcTemplate = new JdbcTemplate(ds);
	}
		
	@Override
	public <ObjectType> QueryStatement<ObjectType> getQuery(JdbcTemplate  jdbcTemplate, Class<ObjectType> objectClass, String sql ) {
		return new BasicQueryStatement<ObjectType>( objectClass, jdbcTemplate, sql );
	}

	@Override
	public <ObjectType> UpdateStatement<ObjectType> getUpdate(JdbcTemplate jdbcTemplate, Class<ObjectType> objectClass, String sql, String... properties ) {
		return new BasicUpdateStatement<ObjectType>( objectClass, jdbcTemplate, sql, properties );
	}

	@Override
	public <ObjectType> UpdateStatement<ObjectType> getInsert(JdbcTemplate jdbcTemplate, Class<ObjectType> objectClass, String table, String... fields ) {
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append( "INSERT INTO " );
		sqlBuilder.append( table );
		sqlBuilder.append( " (" );

		if ( fields.length > 0 ) {
			sqlBuilder.append( "" );
			sqlBuilder.append( fields[0] );
			for ( int i = 1; i < fields.length; ++i ) {
				sqlBuilder.append( "," );
				sqlBuilder.append( fields[i] );
			}
			sqlBuilder.append( "" );
		}
		
		sqlBuilder.append( ") VALUES (" );
		if ( fields.length > 0 ) {
			sqlBuilder.append( "?" );
			for ( int i = 1; i < fields.length; ++i ) {
				sqlBuilder.append( ",?" );
			}
		}
		sqlBuilder.append( ")" );
		
		return new BasicUpdateStatement<ObjectType>( objectClass, jdbcTemplate, sqlBuilder.toString(), fields );
	}

	@Override
	public <ObjectType> UpdateStatement<ObjectType> getUpdate(Class<ObjectType> objectClass, String sql, String... properties) {
		return getUpdate(jdbcTemplate, objectClass, sql, properties);
	}

	@Override
	public <ObjectType> UpdateStatement<ObjectType> getInsert(Class<ObjectType> objectClass, String table, String... fields) {
		return getInsert(jdbcTemplate, objectClass, table, fields);
	}

	@Override
	public <ObjectType> QueryStatement<ObjectType> getQuery(Class<ObjectType> objectClass, String sql) {
		return getQuery(jdbcTemplate, objectClass, sql);
	}

}

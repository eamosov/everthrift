package com.knockchat.sql.objects;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public interface ObjectStatementFactory {

	<ObjectType> UpdateStatement<ObjectType> getUpdate( Class<ObjectType> objectClass, String sql, String... properties );
	
	<ObjectType> UpdateStatement<ObjectType> getInsert( Class<ObjectType> objectClass, String table, String... fields );
	
	<ObjectType> QueryStatement<ObjectType> getQuery( Class<ObjectType> objectClass, String sql );
	
	<ObjectType> UpdateStatement<ObjectType> getUpdate(JdbcTemplate jdbcTemplate, Class<ObjectType> objectClass, String sql, String... properties );
	
	<ObjectType> UpdateStatement<ObjectType> getInsert(JdbcTemplate jdbcTemplate, Class<ObjectType> objectClass, String table, String... fields );
	
	<ObjectType> QueryStatement<ObjectType> getQuery(JdbcTemplate jdbcTemplate, Class<ObjectType> objectClass, String sql );
	
}

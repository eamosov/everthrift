package com.knockchat.sql.mapper;

import com.knockchat.sql.mapper.mcb.McbFixedOrderSqlMapper;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public interface SqlMapperFactory {

	<ObjectType> McbFixedOrderSqlMapper<ObjectType> getFixedOrderSqlMapper( Class<ObjectType> objectClass, String... properties );
	
	<ObjectType> SqlMapper<ObjectType> getLabelledSqlMapper( Class<ObjectType> objectClass, String query );
}

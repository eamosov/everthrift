package com.knockchat.sql.mapper.mcb;

import java.util.HashMap;

import com.knockchat.sql.mapper.SqlMapper;
import com.knockchat.sql.mapper.SqlMapperFactory;
import com.knockchat.utils.Pair;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class McbSqlMapperFactory implements SqlMapperFactory {
	
	public static final McbSqlMapperFactory INSTANCE = new McbSqlMapperFactory();
	
	@SuppressWarnings("rawtypes")
	private static final HashMap<Pair<Class,  String>,McbLabelledSqlMapper<?>> cachedSqlMappers = new HashMap<Pair<Class,  String>,McbLabelledSqlMapper<?>>();

	@Override
	public <ObjectType> McbFixedOrderSqlMapper<ObjectType> getFixedOrderSqlMapper( Class<ObjectType> objectClass, String... properties ) {
		return new McbFixedOrderSqlMapper<ObjectType>( objectClass, properties );
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	synchronized public <ObjectType> SqlMapper<ObjectType> getLabelledSqlMapper( Class<ObjectType> objectClass, String query ) {
		
		final Pair<Class, String> k = new Pair<Class, String>(objectClass, query);
		McbLabelledSqlMapper m = cachedSqlMappers.get( k );
		
		if ( m == null) {
			m = new  McbLabelledSqlMapper<ObjectType>( objectClass, query );
			cachedSqlMappers.put( k, m );
		}
		
		return m;
	}

}

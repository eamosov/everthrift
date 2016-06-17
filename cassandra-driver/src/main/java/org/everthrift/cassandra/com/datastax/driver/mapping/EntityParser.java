package org.everthrift.cassandra.com.datastax.driver.mapping;

public interface EntityParser {

	<T> EntityMapper<T> parseEntity(Class<T> entityClass, EntityMapper.Factory factory, MappingManager mappingManager);
	
	<T> MappedUDTCodec<T> parseUDT(Class<T> udtClass, EntityMapper.Factory factory, MappingManager mappingManager);
}

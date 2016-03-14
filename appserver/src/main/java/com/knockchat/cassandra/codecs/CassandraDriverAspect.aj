package com.knockchat.cassandra.codecs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;
import com.google.common.reflect.TypeToken;

public aspect CassandraDriverAspect {
	
	private static final Logger log = LoggerFactory.getLogger("cassandra-query-log");
	
	Object around(CodecRegistry registry, DataType cqlType, TypeToken javaType): this(registry) && execution(private TypeCodec CodecRegistry.maybeCreateCodec(DataType, TypeToken)) && args(cqlType, javaType){
		final Object ret =  proceed(registry, cqlType, javaType);
		return ret !=null ? ret : MoreCodecRegistry.INSTANCE.lookupCodec(cqlType, javaType.getRawType());
	}
	
}

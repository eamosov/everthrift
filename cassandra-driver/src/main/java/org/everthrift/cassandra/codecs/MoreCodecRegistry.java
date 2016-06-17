package org.everthrift.cassandra.codecs;

import java.util.List;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;
import com.google.common.collect.Lists;

public class MoreCodecRegistry {
	
	public static final MoreCodecRegistry INSTANCE = new MoreCodecRegistry();
	
	static {
		INSTANCE.registerFactory(TBaseModelCodec.factory);
		INSTANCE.registerFactory(DateCodec.factory);
		INSTANCE.registerFactory(TEnumCodec.factory);
	}
	
	private MoreCodecRegistry(){
		
	}
	
	@SuppressWarnings("rawtypes")
	private final List<TypeCodecFactory> factories = Lists.newArrayList();
	
	synchronized public <T> void registerFactory(TypeCodecFactory<T> f){
		factories.add(f);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	synchronized public <T> TypeCodec<T> lookupCodec(DataType cqlType, Class<?> javaType){
		for (TypeCodecFactory f: factories){
			if (f.accepts(cqlType) && f.accepts(javaType))
				return f.create(cqlType, javaType);
		}		
		return null;
	}
	
}

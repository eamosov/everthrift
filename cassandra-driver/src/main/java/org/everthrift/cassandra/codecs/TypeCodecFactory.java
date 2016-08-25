package org.everthrift.cassandra.codecs;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;

public interface TypeCodecFactory<T> {

    boolean accepts(Class<?> javaType);

    boolean accepts(DataType cqlType);

    TypeCodec<T> create(DataType cqlType, Class<?> javaType);
}

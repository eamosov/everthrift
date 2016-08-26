package org.everthrift.cassandra.codecs;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.google.common.reflect.TypeToken;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

@Aspect
public class CassandraDriverAspect {

    @Around("this(registry) && execution(private com.datastax.driver.core.TypeCodec com.datastax.driver.core.CodecRegistry.maybeCreateCodec(com.datastax.driver.core.DataType, com.google.common.reflect.TypeToken)) && args(cqlType, javaType)")
    public Object patchLookupCodec(ProceedingJoinPoint pjp, CodecRegistry registry, DataType cqlType, TypeToken javaType) throws Throwable {
        final Object ret = pjp.proceed(new Object[]{registry, cqlType, javaType});
        return ret != null ? ret : MoreCodecRegistry.INSTANCE.lookupCodec(cqlType, javaType.getRawType());
    }
}

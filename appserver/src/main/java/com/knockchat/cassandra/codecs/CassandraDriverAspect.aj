package com.knockchat.cassandra.codecs;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

public aspect CassandraDriverAspect {
	
	private static final Logger log = LoggerFactory.getLogger("cassandra-query-log");
	
	Object around(CodecRegistry registry, DataType cqlType, TypeToken javaType): this(registry) && execution(private TypeCodec CodecRegistry.maybeCreateCodec(DataType, TypeToken)) && args(cqlType, javaType){
		final Object ret =  proceed(registry, cqlType, javaType);
		return ret !=null ? ret : MoreCodecRegistry.INSTANCE.lookupCodec(cqlType, javaType.getRawType());
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	Object around(Session sessionManager, Statement statement): this(sessionManager) && execution(public ResultSetFuture Session+.executeAsync(Statement)) && args(statement){
		
		final long start = System.currentTimeMillis();
		final ResultSetFuture ret =  (ResultSetFuture)proceed(sessionManager, statement);
				
		if (log.isDebugEnabled()){
			
			Futures.addCallback(ret, new FutureCallback(){

				@Override
				public void onSuccess(Object result) {
					
					final long millis = System.currentTimeMillis() - start;
					if (statement instanceof BoundStatement){			
						final List args = new ArrayList();				
						for (int i=0; i<((BoundStatement)statement).preparedStatement().getVariables().size(); i++ ){
							args.add(((BoundStatement)statement).getObject(i));
						}				
						log.debug("Executing query '{}' with args {} in {} millis", ((BoundStatement)statement).preparedStatement().getQueryString(), args, millis);
					}else{
						log.debug("Executing query '{}' in {} millis", statement, millis);
					}										
				}

				@Override
				public void onFailure(Throwable t) {
					// TODO Auto-generated method stub
					
				}});
			
		}
		return ret;
	}

}

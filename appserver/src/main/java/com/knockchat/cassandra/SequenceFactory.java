package com.knockchat.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class SequenceFactory {
	public static final String NAME_COLUMN = "name";
	public static final String VALUE_COLUMN = "value";
	
	private final Session session;
	private final String tableName;	
	private final String seqName;
	
	public SequenceFactory(Session session, String tableName, String seqName) {
		super();
		this.session = session;
		this.tableName = tableName;
		this.seqName = seqName;
	}
	
	public long nextId(){
		
		Long result = null;
		while(result == null){
			result = tryNextId();
		}		
		return result;
	}
	
	public void set(long value){
		session.execute(QueryBuilder.insertInto(tableName).value(NAME_COLUMN, seqName).value(VALUE_COLUMN, value).setConsistencyLevel(ConsistencyLevel.QUORUM));
	}
	
	private Long tryNextId(){
		
		final ResultSet rs = session.execute(QueryBuilder.select(VALUE_COLUMN).from(tableName).where(eq(NAME_COLUMN, seqName)).setConsistencyLevel(ConsistencyLevel.QUORUM));
		final Row row = rs.one();
		if (row == null){
			final ResultSet saveResult = session.execute(QueryBuilder.insertInto(tableName).value(NAME_COLUMN, seqName).value(VALUE_COLUMN, 1L).ifNotExists().setSerialConsistencyLevel(ConsistencyLevel.SERIAL));
			if (saveResult.wasApplied())
				return 1L;
		}else{
			long value = row.getLong(0);
			final ResultSet updateResult = session.execute(QueryBuilder.update(tableName).with(QueryBuilder.set(VALUE_COLUMN, (Long)(value+1))).where(eq(NAME_COLUMN, seqName)).onlyIf(eq(VALUE_COLUMN, value)).setSerialConsistencyLevel(ConsistencyLevel.SERIAL));
			if (updateResult.wasApplied())
				return value+1;
		}
		
		return null;
	}

}

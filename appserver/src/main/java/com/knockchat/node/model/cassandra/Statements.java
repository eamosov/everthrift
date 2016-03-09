package com.knockchat.node.model.cassandra;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.thrift.TException;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.Mapper.Option;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.node.model.events.DeleteEntityEvent;
import com.knockchat.node.model.events.InsertEntityEvent;
import com.knockchat.node.model.events.UpdateEntityEvent;
import com.knockchat.utils.thrift.TFunction;

public class Statements {
	
	
	private List<Statement> statements = Lists.newArrayList();
	
	@SuppressWarnings({ "rawtypes"})
	private Multimap<CassandraModelFactory, Object> invalidates = Multimaps.newSetMultimap(Maps.newIdentityHashMap(), () -> Sets.newHashSet()) ;
	private List<Runnable> callbacks = Lists.newArrayList();
	private Session session;
	private boolean autoCommit = false;
	private boolean isBatch = true;
	private long timestamp; //in microseconds
	
	private CassandraFactories cassandraFactories;
	
	Statements(CassandraFactories cassandraFactories, Session session){
		this.cassandraFactories = cassandraFactories;
		this.session = session;
		this.timestamp = System.currentTimeMillis() * 1000;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <PK extends Serializable,ENTITY extends DaoEntityIF, E extends TException> CassandraModelFactory<PK, ENTITY, E> of(ENTITY e){
		return (CassandraModelFactory)cassandraFactories.of(e.getClass());
	}
	
	@SuppressWarnings({ "rawtypes"})
	private <ENTITY extends DaoEntityIF> void addStatement(final CassandraModelFactory f, final Statement s, ENTITY e){
		if (s!=null){
			statements.add(s);
			invalidates.put(f, e.getPk());
		}		
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <ENTITY extends DaoEntityIF> ENTITY update(ENTITY e, TFunction<ENTITY, Boolean> mutator, Option... options) throws TException{
		final CassandraModelFactory f = of(e);
		final ENTITY orig = (ENTITY)f.copy(e);
		addStatement(f, f.updateQuery(orig, e, mutator, ArrayUtils.add(options, Option.updatedAt(timestamp/1000))), e);
		
		final ENTITY copy = (ENTITY)f.copy(e);
		final UpdateEntityEvent event = f.updateEntityEvent(orig, copy);
		callbacks.add(()->{
			f.localEventBus.postAsync(event);
		});
		
		if (autoCommit)
			commit();
		
		return e;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <ENTITY extends DaoEntityIF> ENTITY save(ENTITY e, Option... options){
		final CassandraModelFactory f = of(e);
		addStatement(f, f.insertQuery(e, options), e);
		
		final ENTITY copy = (ENTITY)f.copy(e);
		final InsertEntityEvent event = f.insertEntityEvent(copy);
		callbacks.add(()->{
			f.localEventBus.postAsync(event);			
		});
		
		if (autoCommit)
			commit();
		
		return e;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <ENTITY extends DaoEntityIF> ENTITY delete(ENTITY e, Option... options){
		final CassandraModelFactory f = of(e);
		
		final Serializable pk = e.getPk();
		statements.add(f.deleteQuery(pk, options));
		invalidates.put(f, pk);

		final DeleteEntityEvent event = f.deleteEntityEvent(e);
		callbacks.add(()->{
			f.localEventBus.postAsync(event);			
		});
		
		if (autoCommit)
			commit();

		return e;
	}

	public void add(Statement s){
		statements.add(s);
		
		if (autoCommit)
			commit();
	}
	
	@SuppressWarnings({ "rawtypes" })
	public <ENTITY extends DaoEntityIF> void invalidate(ENTITY e){
		invalidate(of(e), e.getPk());
	}

	@SuppressWarnings({ "rawtypes" })
	public void invalidate(CassandraModelFactory f, Object key){
		invalidates.put(f, key);
		
		if (autoCommit)
			commit();		
	}

	public void add(Runnable successCallback){
		callbacks.add(() -> {successCallback.run();});
		
		if (autoCommit)
			commit();
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ListenableFuture<?> commitAsync(Executor executor){		
		final ListenableFuture f;
		
		if (isBatch() && statements.size()>1){
			final BatchStatement batch = new BatchStatement();
			batch.setDefaultTimestamp(timestamp);
			statements.forEach(s -> batch.add(s));
			f = session.executeAsync(batch);			
		}else{
			final List<ResultSetFuture> ff = Lists.newArrayList();
			for (Statement s: statements){
				ff.add(session.executeAsync(s));
			}
			f = Futures.successfulAsList(ff);
		}
		
		Futures.addCallback(f, new FutureCallback<Object>(){

			@Override
			public void onSuccess(Object result) {
				for (Map.Entry<CassandraModelFactory, Object> e: invalidates.entries()){
					e.getKey().invalidate(e.getValue());
				}
				
				callbacks.forEach(c -> c.run());
			}

			@Override
			public void onFailure(Throwable t) {
				t.printStackTrace();
			}});
						
		return f;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void commit(){
		
		if (isBatch() && statements.size()>1){
			final BatchStatement batch = new BatchStatement();
			batch.setDefaultTimestamp(timestamp);
			statements.forEach(s -> batch.add(s));
			session.execute(batch);			
		}else{
			for (Statement s: statements){
				session.execute(s);
			}
		}
		
		for (Map.Entry<CassandraModelFactory, Object> e: invalidates.entries()){
			e.getKey().invalidate(e.getValue());
		}
				
		callbacks.forEach(c -> c.run());
		
		statements.clear();
		invalidates.clear();
		callbacks.clear();		
	}

	public boolean isAutoCommit() {
		return autoCommit;
	}

	public Statements setAutoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
		return this;
	}

	public boolean isBatch() {
		return isBatch;
	}

	public Statements setBatch(boolean isBatch) {
		this.isBatch = isBatch;
		return this;
	}

	public long getTimestamp() {
		return timestamp;
	}
	
	public long currentTimeMillis(){
		return timestamp / 1000;
	}

//	public void setTimestamp(long timestamp) {
//		this.timestamp = timestamp;
//	}
	
//	public Statements append(Statements s){
//		
//		if (s.statements !=null)
//			statements.addAll(s.statements);
//		
//		if (s.onExecute !=null)
//			onExecute.addAll(s.onExecute);
//
//		return this;
//	}
//	
//	public Statements prepend(Statements s){
//		
//		s.append(this);
//		return s;
//	}
//	
//	public ListenableFuture<ResultSet> executeAsync(Session session, Executor executor){
//		
//		final ResultSetFuture f;
//		
//		if (!CollectionUtils.isEmpty(statements)){
//			f = session.executeAsync(batch());
//			Futures.addCallback(f, new FutureCallback<ResultSet>(){
//
//				@Override
//				public void onSuccess(ResultSet result) {
//					run();
//				}
//
//				@Override
//				public void onFailure(Throwable t) {
//					t.printStackTrace();
//				}}, executor);
//			
//			return f;
//		}else{
//			run();
//			return Futures.immediateFuture((ResultSet)null);
//		}
//	}
//	
//	public ResultSet execute(Session session, Executor executor){
//		try {
//			return executeAsync(session, executor).get();
//		} catch (InterruptedException e) {
//			throw new RuntimeException(e);
//		} catch (ExecutionException e) {
//			throw Throwables.propagate(e.getCause());
//		}
//	}
//	
//	private BatchStatement batch(){
//		final BatchStatement batch = new BatchStatement();
//		statements.forEach(s -> batch.add(s));
//		return batch;
//	}
//	
//	public void run(){
//		if (!CollectionUtils.isEmpty(onExecute))
//			onExecute.forEach(r -> r.run());
//	}
}

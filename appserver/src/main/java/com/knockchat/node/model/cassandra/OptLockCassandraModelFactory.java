package com.knockchat.node.model.cassandra;

import java.io.Serializable;

import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TException;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import com.datastax.driver.mapping.ColumnMapper;
import com.datastax.driver.mapping.Mapper.Option;
import com.datastax.driver.mapping.VersionException;
import com.google.common.base.Throwables;
import com.knockchat.appserver.model.CreatedAtIF;
import com.knockchat.appserver.model.UpdatedAtIF;
import com.knockchat.cassandra.DLock;
import com.knockchat.cassandra.SequenceFactory;
import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.node.model.EntityFactory;
import com.knockchat.node.model.EntityNotFoundException;
import com.knockchat.node.model.OptResult;
import com.knockchat.node.model.OptimisticLockModelFactoryIF;
import com.knockchat.node.model.UniqueException;
import com.knockchat.utils.VoidFunction;
import com.knockchat.utils.thrift.TFunction;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

public abstract class OptLockCassandraModelFactory<PK extends Serializable,ENTITY extends DaoEntityIF, E extends TException> extends CassandraModelFactory<PK, ENTITY, E> implements OptimisticLockModelFactoryIF<PK, ENTITY, E>{

	private String sequenceName;
	private SequenceFactory sequenceFactory;			
			
	public OptLockCassandraModelFactory(Cache cache, Class<ENTITY> entityClass) {
		super(cache, entityClass);
	}
	
	public OptLockCassandraModelFactory(String cacheName, Class<ENTITY> entityClass) {
		super(cacheName, entityClass);
	}
		
	@Override
	protected void initMapping(){
	
		super.initMapping();
				
		if (sequenceName == null){
			sequenceFactory = null;
		}else{
			sequenceFactory = new SequenceFactory(mappingManager.getSession(), "sequences", sequenceName);
		}
	}
	
	
	public final void setPkSequenceName(String sequenceName){
		this.sequenceName = sequenceName;
		
		if (sequenceName == null){
			sequenceFactory = null;
		}else if (mappingManager !=null){
			sequenceFactory = new SequenceFactory(mappingManager.getSession(), "sequences", sequenceName);
		}
	}

	@Override
	public final ENTITY insertEntity(ENTITY e) {
		return optInsert(e).afterUpdate;
	}

	@Override
	public final void deleteEntity(ENTITY e) {
		_delete(e);
	}

	@Override
	public final OptResult<ENTITY> updateUnchecked(PK id, TFunction<ENTITY, Boolean> mutator) {
		return updateUnchecked(id, mutator, null);
	}

	@Override
	public final OptResult<ENTITY> updateUnchecked(PK id, TFunction<ENTITY, Boolean> mutator, EntityFactory<PK, ENTITY> factory) {
		try {
			return update(id, mutator, factory);
		} catch (TException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final OptResult<ENTITY> update(PK id, TFunction<ENTITY, Boolean> mutator) throws TException, E {
		return update(id, mutator, null);
	}
		
	private  Number fetchCurrentVersion(PK id, final Object[] pkeys, final ColumnMapper<ENTITY> versionColumn) throws EntityNotFoundException{
		final Select.Where where = QueryBuilder.select(versionColumn.getColumnNameUnquoted()).from(mapper.getTableName()).where();
		
		for (int i=0; i< mapper.primaryKeySize(); i++){
			where.and(QueryBuilder.eq(mapper.getPrimaryKeyColumn(i).getColumnNameUnquoted(), pkeys[i]));
		}
		where.setConsistencyLevel(ConsistencyLevel.SERIAL);
		final ResultSet rs = getSession().execute(where);
		final Row row = rs.one();
		if (row == null)
			throw new EntityNotFoundException(id);
		
        final TypeCodec<Object> customCodec = versionColumn.getCustomCodec();
        if (customCodec != null)
        	return (Number)row.get(0, customCodec);
        else
        	return (Number)row.get(0, versionColumn.getJavaType());					
		
	}
	
	public final OptResult<ENTITY> updateWithAssignments(PK id, VoidFunction<Assignments> assignment) throws TException, E {
		
		if (id == null)
			throw new IllegalArgumentException("id is null");
		
		if (mapper.getVersionColumn() == null)
			throw new IllegalArgumentException("version not configured");
		
		try {
			final OptResult<ENTITY> optResult = OptimisticLockModelFactoryIF.optimisticUpdate((count) -> {

				final ColumnMapper<ENTITY> versionColumn = mapper.getVersionColumn();
				final Object[] pkeys = extractCompaundPk(id);

	            final Number versionBefore;

				if (count == 0 && getCache() != null){
					final Element e = getCache().get(id);
					if (e != null && e.getObjectValue() != null){
						versionBefore = (Number)versionColumn.getValue((ENTITY)e.getObjectValue());
					}else{
						versionBefore = fetchCurrentVersion(id, pkeys, versionColumn);
					}
				}else{
					versionBefore = fetchCurrentVersion(id, pkeys, versionColumn);				
				}				
	            	            
	            final Number versionAfter;
               	if (versionBefore instanceof Integer){
               		versionAfter = (Integer) versionBefore + 1;
               	}else if (versionBefore instanceof Long){
               		versionAfter = (Long) versionBefore + 1;
               	}else{
               		throw new RuntimeException("invalid type for version column: " + versionBefore.getClass().getCanonicalName());
               	}
               	
               	final Update update = QueryBuilder.update(mapper.getTableName());
               	final Assignments assignments = update.with();
               	assignments.and(QueryBuilder.set(versionColumn.getColumnNameUnquoted(), versionAfter));
               	assignment.apply(assignments);
               	update.onlyIf(QueryBuilder.eq(versionColumn.getColumnNameUnquoted(), versionBefore));
               	
               	final Update.Where uWhere = update.where();
				for (int i=0; i< mapper.primaryKeySize(); i++){
					uWhere.and(QueryBuilder.eq(mapper.getPrimaryKeyColumn(i).getColumnNameUnquoted(), pkeys[i]));
				}
               	
				final ResultSet rs = getSession().execute(update);
               	
               	if (!rs.wasApplied())
               		return null;
               	
   				invalidate(id);

				try {
					final ENTITY beforeUpdate = getEntityClass().newInstance();
	               	beforeUpdate.setPk(id);
	               	versionColumn.setValue(beforeUpdate, versionBefore);
	               	
	               	final ENTITY afterUpdate = getEntityClass().newInstance();
	               	afterUpdate.setPk(id);
	               	versionColumn.setValue(afterUpdate, versionAfter);
	               	
	               	return OptResult.create(OptLockCassandraModelFactory.this, afterUpdate, beforeUpdate, true);					
				} catch (Exception e) {
					throw Throwables.propagate(e);
				}
			});
						
			if (optResult.isUpdated){
				localEventBus.postAsync(updateEntityEvent(optResult.beforeUpdate, optResult.afterUpdate));
			}
			
			return optResult;
		} catch (EntityNotFoundException e) {
			throw createNotFoundException(id); 
		}
	}
	
//	private BoundStatement bindPk(final PK id, final BoundStatement bs){
//		final Object[] pkeys = extractCompaundPk(id);
//
//		for (int i=0; i< mapper.primaryKeySize(); i++){
//			final ColumnMapper<ENTITY> cm = mapper.getPrimaryKeyColumn(i);
//	        final TypeCodec<Object> customCodec = cm.getCustomCodec();
//	        if (customCodec != null)
//	            bs.set(i, pkeys[i], customCodec);
//	        else
//	            bs.set(i, pkeys[i], cm.getJavaType());
//		}
//		return bs;
//	}
//
//	/**
//	 * Make update request, no cache validation
//	 * @param id
//	 * @param assignment
//	 * @throws TException
//	 * @throws E
//	 */
//	public final void updateCustom(PK id, VoidFunction<Assignments> assignment) throws TException, E {
//		
//		if (id == null)
//			throw new IllegalArgumentException("id is null");
//		
//              	
//		final Update update = QueryBuilder.update(mapper.getTableName());
//		final Assignments assignments = update.with();
//		assignment.apply(assignments);
//               	
//		final Update.Where uWhere = update.where();
//		for (int i=0; i< mapper.primaryKeySize(); i++){
//			uWhere.and(QueryBuilder.eq(mapper.getPrimaryKeyColumn(i).getColumnNameUnquoted(), QueryBuilder.bindMarker()));
//		}
//		
//		update.setConsistencyLevel(mapper.getWriteConsistency());
//		
//		final BoundStatement bs = getSession().prepare(update).bind();
//
//		getSession().execute(bindPk(id, bs));
//		return ;
//	}
//	
//	public final ResultSet selectCustom(PK id, String ...columns){
//		if (id == null)
//			throw new IllegalArgumentException("id is null");
//		
//		final Object[] pkeys = extractCompaundPk(id);
//		final Select select = QueryBuilder.select(columns).from(mapper.getTableName()); 
//
//		final Select.Where uWhere = select.where();
//		for (int i=0; i< mapper.primaryKeySize(); i++){
//			uWhere.and(QueryBuilder.eq(mapper.getPrimaryKeyColumn(i).getColumnNameUnquoted(), QueryBuilder.bindMarker()));
//		}
//		select.setConsistencyLevel(mapper.getReadConsistency());
//		
//		final BoundStatement bs = getSession().prepare(select).bind();
//
//		return getSession().execute(bindPk(id, bs));
//	}
	
	@Override
	public final OptResult<ENTITY> update(PK id, TFunction<ENTITY, Boolean> mutator, EntityFactory<PK, ENTITY> factory) throws TException, E {
		
		if (id == null)
			throw new IllegalArgumentException("id is null");
		
		try {
			final OptResult<ENTITY> ret = OptimisticLockModelFactoryIF.optimisticUpdate((count) -> {
				
				final long now = System.currentTimeMillis();
				
				ENTITY e;
				
				if (count == 0)
					e = findEntityById(id);
				else
					e = fetchEntityById(id);
				
				final ENTITY orig;
				
				if (e == null){
					if (factory == null)
						throw new EntityNotFoundException(id);
					
					e = factory.create(id);
					
					if (e instanceof CreatedAtIF && (((CreatedAtIF)e).getCreatedAt() == 0))
			        	((CreatedAtIF)e).setCreatedAt(now);
			        					
					orig = null;
				}else{
					try {
						orig = getCopyConstructor().newInstance(e);
					} catch (Exception e1) {
						throw new RuntimeException(e1);
					}
				}
				
				final boolean needUpdate = mutator.apply(e);
				if (needUpdate == false)
					return OptResult.create(OptLockCassandraModelFactory.this, e, orig, false);
				
				
//		        if (e instanceof UpdatedAtIF)
//		        	((UpdatedAtIF) e).setUpdatedAt(now);

	        	if (orig == null){
	        		
	        		final DLock lock = this.assertUnique(null, e);
	        		final boolean saved;
	        		try{
	        			saved = mapper.save(e, Option.ifNotExist());			
	        		}finally{
	        			if (lock !=null)
	        				lock.unlock();
	        		}

	        		if (saved){
	        			invalidate(id);
	        			return OptResult.create(OptLockCassandraModelFactory.this, e, null, true);
	        		}else{
			        	if (count == 0)
			        		invalidate(id);
	        			return null;
	        		}
	        	}else{
	        		try{
	        			
	        			final DLock lock = this.assertUnique(orig, e);
	        			final boolean updated;
		        		try{
		        			updated = mapper.update(e, orig, Option.onlyIf());			
		        		}finally{
		        			if (lock !=null)
		        				lock.unlock();
		        		}
	        			
	        			if (updated)
	        				invalidate(id);
	        			
	        			return OptResult.create(OptLockCassandraModelFactory.this, e, orig, updated);
	        		}catch(VersionException ve){
	        			
			        	if (count == 0)
			        		invalidate(id);
			        	
			        	return null;
			        }
	        	}
			});
			
			if (ret.isUpdated){
				localEventBus.postAsync(updateEntityEvent(ret.beforeUpdate, ret.afterUpdate));
			}
			
			return ret;
		} catch (EntityNotFoundException e) {
			throw createNotFoundException(id);
		}				
	}

	private OptResult<ENTITY> _delete(ENTITY e){				
		mapper.delete(e);
		invalidate((PK)e.getPk());
		final OptResult<ENTITY> r= OptResult.create(this, null, e, true);; 
		localEventBus.postAsync(deleteEntityEvent(e));		
		return r;
	}

	@Override
	public final OptResult<ENTITY> delete(PK id) throws E {
		
		final ENTITY e = findEntityById(id);
		
		if (e == null)
			throw createNotFoundException(id);
		
		return _delete(e);
	}
	
	@Override
	public final OptResult<ENTITY> optInsert(ENTITY e) {
				
		final long now = System.currentTimeMillis();
		
		if (e.getPk() == null){
			if (sequenceFactory !=null)
				e.setPk(sequenceFactory.nextId());
			else
				throw new IllegalArgumentException("PK is null:" + e.toString());
		}
        
		if (e instanceof CreatedAtIF && (((CreatedAtIF)e).getCreatedAt() == 0))
        	((CreatedAtIF)e).setCreatedAt(now);
        
        if (e instanceof UpdatedAtIF)
        	((UpdatedAtIF) e).setUpdatedAt(now);

		final DLock lock = this.assertUnique(null, e);
		final boolean saved;
		try{
			saved = mapper.save(e, Option.ifNotExist());			
		}finally{
			if (lock !=null)
				lock.unlock();
		}
        
		invalidate((PK)e.getPk());
				
		if (saved == false)
			throw new UniqueException(null, true, null);
		
		final OptResult<ENTITY> r = OptResult.create(this, e, null, true); 

		localEventBus.postAsync(insertEntityEvent(e));
		
		return r;
	}

	@Override
	public ENTITY updateEntity(ENTITY e) throws UniqueException {
		throw new NotImplementedException();
	}
	
	/**
	 * Unsafely assume insert without check for update. Use only with uniq (random) PK
	 * @param e
	 * @return
	 */
	public final OptResult<ENTITY> fastInsert(ENTITY e) {
		final long now = System.currentTimeMillis();
		
		if (e instanceof CreatedAtIF && (((CreatedAtIF)e).getCreatedAt() == 0))
        	((CreatedAtIF)e).setCreatedAt(now);
        
        if (e instanceof UpdatedAtIF)
        	((UpdatedAtIF) e).setUpdatedAt(now);
	
        putEntity(e, false);
		final OptResult<ENTITY> r = OptResult.create(this, e, null, true); 

		localEventBus.postAsync(insertEntityEvent(e));		
		return r;        
	}
}

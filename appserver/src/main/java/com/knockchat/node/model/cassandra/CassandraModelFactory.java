package com.knockchat.node.model.cassandra;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.ColumnMapper;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.Mapper.Option;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.VersionException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.knockchat.appserver.model.CreatedAtIF;
import com.knockchat.appserver.model.Unique;
import com.knockchat.appserver.model.UpdatedAtIF;
import com.knockchat.cassandra.DLock;
import com.knockchat.cassandra.DLockFactory;
import com.knockchat.cassandra.SequenceFactory;
import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.node.model.AbstractCachedModelFactory;
import com.knockchat.node.model.EntityFactory;
import com.knockchat.node.model.EntityNotFoundException;
import com.knockchat.node.model.LocalEventBus;
import com.knockchat.node.model.OptResult;
import com.knockchat.node.model.OptimisticLockModelFactoryIF;
import com.knockchat.node.model.UniqueException;
import com.knockchat.node.model.events.DeleteEntityEvent.SyncDeleteEntityEvent;
import com.knockchat.node.model.events.InsertEntityEvent.SyncInsertEntityEvent;
import com.knockchat.node.model.events.UpdateEntityEvent.SyncUpdateEntityEvent;
import com.knockchat.utils.Pair;
import com.knockchat.utils.thrift.TFunction;

import net.sf.ehcache.Cache;

public abstract class CassandraModelFactory<PK extends Serializable,ENTITY extends DaoEntityIF, E extends TException> extends AbstractCachedModelFactory<PK, ENTITY> implements OptimisticLockModelFactoryIF<PK, ENTITY, E>{

	private final Class<ENTITY> entityClass;
	private final Constructor<ENTITY> copyConstructor;

	@Autowired
	private MappingManager mappingManager;
	
	@Autowired
	protected LocalEventBus localEventBus;
	
	private Mapper<ENTITY> mapper;
	private String sequenceName;
	private SequenceFactory sequenceFactory;
	private DLockFactory dLockFactory;
	
	private final static Option noSaveNulls = Option.saveNullFields(false);

	
	protected abstract E createNotFoundException(PK id);
	
	private final List<Pair<ColumnMapper<ENTITY>, PreparedStatement>> uniqueColumns = Lists.newArrayList();
			
	public CassandraModelFactory(Cache cache, Class<ENTITY> entityClass) {
		super(cache);
		this.entityClass  = entityClass;
		
		try {
			copyConstructor = this.entityClass.getConstructor(this.entityClass);
		} catch (NoSuchMethodException | SecurityException e) {
			throw new RuntimeException(e);
		}
		
	}
	
	public CassandraModelFactory(String cacheName, Class<ENTITY> entityClass) {
		super(cacheName);
		this.entityClass  = entityClass;
		
		try {
			copyConstructor = this.entityClass.getConstructor(this.entityClass);
		} catch (NoSuchMethodException | SecurityException e) {
			throw new RuntimeException(e);
		}
		
	}
	
	public final void setMappingManager(MappingManager mappingManager){
		this.mappingManager = mappingManager;
		initMapping();
	}
	
	public final void setLocalEventBus(LocalEventBus localEventBus){
		this.localEventBus = localEventBus;
		this.localEventBus.register(this);
	}
	
	private void initMapping(){
	
		dLockFactory  = new DLockFactory(mappingManager.getSession());
		mapper = mappingManager.mapper(this.entityClass);
		
		for (Unique u: entityClass.getAnnotationsByType(Unique.class)){
			final ColumnMapper<ENTITY> cm = mapper.getColumnByFieldName(u.value());
			if (cm == null)
				throw new RuntimeException(String.format("coundn't find ColumnMapper for unique filed %s in class %s", u.value(), entityClass.getSimpleName()));
			
			final StringBuilder query = new StringBuilder();
			query.append(String.format("SELECT %s FROM %s WHERE %s=?", mapper.getPrimaryKeyColumn(0).getColumnName(), mapper.getTableName(), cm.getColumnName()));
			if (!u.clause().isEmpty()){
				query.append(" AND ");
				query.append(u.clause());
				query.append(" ALLOW FILTERING");
			}
			final PreparedStatement ps = mappingManager.getSession().prepare(query.toString());						
			uniqueColumns.add(Pair.create(cm, ps));
		}
		
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
		return optInsert(e).updated;
	}

	@Override
	public final void deleteEntity(ENTITY e) {
		_delete(e);
	}

	@Override
	public final Class<ENTITY> getEntityClass() {
		return entityClass;
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
	
	protected Object[] extractCompaundPk(PK id){
		return new Object[]{id};
	}

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
						orig = copyConstructor.newInstance(e);
					} catch (Exception e1) {
						throw new RuntimeException(e1);
					}
				}
				
				final boolean needUpdate = mutator.apply(e);
				if (needUpdate == false)
					return OptResult.create(CassandraModelFactory.this, e, orig, false);
				
				
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
	        			return OptResult.create(CassandraModelFactory.this, e, null, true);
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
	        			
	        			return OptResult.create(CassandraModelFactory.this, e, orig, updated);
	        		}catch(VersionException ve){
	        			
			        	if (count == 0)
			        		invalidate(id);
			        	
			        	return null;
			        }
	        	}
			});
			
			if (ret.isUpdated){
				localEventBus.post(syncUpdateEntityEvent(ret));
				localEventBus.postAsync(asyncUpdateEntityEvent(ret.old, ret.updated));
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
		localEventBus.post(syncDeleteEntityEvent(r));
		localEventBus.postAsync(asyncDeleteEntityEvent(e));		
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

		localEventBus.post(syncInsertEntityEvent(r));
		localEventBus.postAsync(asyncInsertEntityEvent(e));
		
		return r;
	}

	@Override
	final protected Map<PK, ENTITY> fetchEntityByIdAsMap(Collection<PK> _ids) {
		
		final List<PK> ids = ImmutableList.copyOf(_ids);
		final List<ListenableFuture<ENTITY>> ff = Lists.transform(ids, pk -> (mapper.getAsync(extractCompaundPk(pk))));
		
		try {
			final List<ENTITY> ee =  Futures.successfulAsList(ff).get();
			final Map<PK, ENTITY> ret = Maps.newHashMap();
			for (int i=0; i< ids.size(); i++){
				ret.put(ids.get(i), ee.get(i));
			}
			return ret;
			
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}		
	}

	@Override
	final protected ENTITY fetchEntityById(PK id) {
		return mapper.get(extractCompaundPk(id));
	}
	
	public abstract Iterator<PK> getAllIds();

	public final <T> Iterator<T> getAll(String fieldName){
		
		final Select.Selection select = QueryBuilder.select();
		
		final ColumnMapper<ENTITY> cm = mapper.getColumnByFieldName(fieldName);
		if (cm == null)
			throw new RuntimeException("coundn't find mapper for property: " + fieldName);
		
		final ResultSet rs = mappingManager.getSession().execute(select.column(cm.getColumnName()).from(mapper.getTableMetadata().getName()).setFetchSize(1000));
		
		return Iterators.transform(rs.iterator(), row -> {
			final T value;
            final TypeCodec<Object> customCodec = cm.getCustomCodec();
            if (customCodec != null)
                value = (T)row.get(0, customCodec);
            else
                value = (T)row.get(0, cm.getJavaType());
			
			return value;			
		});
	}
	
	private static class UniqueField<ENTITY>{
		final ColumnMapper<ENTITY> cm;
		final PreparedStatement ps;
		final Object value;
		
		UniqueField(ColumnMapper<ENTITY> cm, PreparedStatement ps, Object value) {
			super();
			this.cm = cm;
			this.ps = ps;
			this.value = value;
		}				
	}
	
	private DLock assertUnique(ENTITY from, ENTITY to){
		
		if (uniqueColumns.isEmpty())
			return null;
		
		final List<UniqueField<ENTITY>> uf = Lists.newArrayList();
		
		for (Pair<ColumnMapper<ENTITY>, PreparedStatement> p: uniqueColumns){
			final Object _to = p.first.getValue(to);
			if (_to !=null && (from == null || !_to.equals(p.first.getValue(from)))){
				uf.add(new UniqueField<ENTITY>(p.first, p.second, _to));
			}
		}
		
		if (uf.isEmpty())
			return null;
		
		final String lockNames[] = new String[uf.size()];
		for(int i=0; i< uf.size(); i++){
			final UniqueField<ENTITY> _u = uf.get(i);
			lockNames[i] = (_u.cm.getColumnNameUnquoted() + ":" + _u.value.toString());
		}

		final DLock lock = dLockFactory.lock(lockNames);
		
		try{
			for (UniqueField<ENTITY> _u: uf){
				final BoundStatement bs = _u.ps.bind();
				
		        final TypeCodec<Object> customCodec = _u.cm.getCustomCodec();
		        if (customCodec != null)
		            bs.set(0, _u.value, customCodec);
		        else
		            bs.set(0, _u.value, _u.cm.getJavaType());
				
				bs.setConsistencyLevel(ConsistencyLevel.SERIAL);
				final Row r = mappingManager.getSession().execute(bs).one();
				if (r !=null)
					throw new UniqueException(String.format("Violate uniqe constraint for field %s, value:%s, pk:%s", _u.cm.getFieldName(), _u.value, r.getObject(0)), _u.cm.getFieldName());							
			}			
		}catch(Exception e){
			lock.unlock();
			throw Throwables.propagate(e);
		}		
		
		return lock;
	}

	@PostConstruct
	private void afterPropertiesSet(){
		initMapping();
		localEventBus.register(this);
	}
	
	public final Session getSession(){
		return this.mappingManager.getSession();
	}
	
	public final Select select(){
		return (Select)QueryBuilder.select().all().from(mapper.getTableName()).setConsistencyLevel(mapper.getReadConsistency());
	}
	
	public final List<ENTITY> findByClause(Statement select){
		final ResultSet rs = mappingManager.getSession().execute(select);
		return mapper.map(rs).all();		
	}
	
	public final ENTITY findOneByClause(Statement select){
		final ResultSet rs = mappingManager.getSession().execute(select);		
		final List<ENTITY> ret = mapper.map(rs).all();
		if (ret.size() > 1)
			throw new RuntimeException("multiple results");
		return ret.isEmpty() ? null : ret.get(0);
	}

	@Override
	final public SyncInsertEntityEvent<PK, ENTITY> syncInsertEntityEvent(ENTITY entity){
		throw new NotImplementedException();
	}
	
	@Override
	final public SyncUpdateEntityEvent<PK, ENTITY> syncUpdateEntityEvent(ENTITY before, ENTITY after){
		throw new NotImplementedException();
	}

	@Override
	final public SyncDeleteEntityEvent<PK, ENTITY> syncDeleteEntityEvent(ENTITY entity){
		throw new NotImplementedException();
	}

	@Override
	public ENTITY updateEntity(ENTITY e) throws UniqueException {
		throw new NotImplementedException();
	}
	
	/*
	 * save all not null fields without read
	 * Method not generates any events
	 */
	public void putEntity(ENTITY entity) {
		if (entity.getPk() == null)
			throw new IllegalArgumentException("pk is null");
		
		mapper.save(entity, noSaveNulls);
		invalidate((PK)entity.getPk());
	}
		
}

package com.knockchat.node.model.cassandra;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.thrift.TException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.ColumnMapper;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.Mapper.Option;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.VersionException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.knockchat.appserver.model.CreatedAtIF;
import com.knockchat.appserver.model.UpdatedAtIF;
import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.node.model.AbstractCachedModelFactory;
import com.knockchat.node.model.EntityFactory;
import com.knockchat.node.model.EntityNotFoundException;
import com.knockchat.node.model.OptResult;
import com.knockchat.node.model.OptimisticLockModelFactoryIF;
import com.knockchat.node.model.RwModelFactoryHelper;
import com.knockchat.node.model.UniqueException;
import com.knockchat.utils.thrift.TFunction;

import net.sf.ehcache.Cache;

public abstract class CassandraModelFactory<PK extends Serializable,ENTITY extends DaoEntityIF, E extends TException> extends AbstractCachedModelFactory<PK, ENTITY> implements OptimisticLockModelFactoryIF<PK, ENTITY, E>{

	private final Class<ENTITY> entityClass;
	private final Constructor<ENTITY> copyConstructor;
	private final MappingManager mappingManager;
	protected final Mapper<ENTITY> mapper;
	
	protected abstract E createNotFoundException(PK id);
	
	public CassandraModelFactory(Cache cache, Class<ENTITY> entityClass, MappingManager mappingManager) {
		super(cache);
		this.entityClass  = entityClass;
		this.mappingManager = mappingManager;
		
		mapper = mappingManager.mapper(this.entityClass);
		try {
			copyConstructor = this.entityClass.getConstructor(this.entityClass);
		} catch (NoSuchMethodException | SecurityException e) {
			throw new RuntimeException(e);
		}		
	}
	
	public CassandraModelFactory(String cacheName, Class<ENTITY> entityClass, MappingManager mappingManager) {
		super(cacheName);
		this.entityClass  = entityClass;
		this.mappingManager = mappingManager;
		
		mapper = mappingManager.mapper(this.entityClass);
		try {
			copyConstructor = this.entityClass.getConstructor(this.entityClass);
		} catch (NoSuchMethodException | SecurityException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public ENTITY insertEntity(ENTITY e) {
		return optInsert(e).updated;
	}

	@Override
	public void deleteEntity(ENTITY e) {
		_delete(e);
	}

	@Override
	public Class<ENTITY> getEntityClass() {
		return entityClass;
	}

	@Override
	public OptResult<ENTITY> updateUnchecked(PK id, TFunction<ENTITY, Boolean> mutator) {
		return updateUnchecked(id, mutator, null);
	}

	@Override
	public OptResult<ENTITY> updateUnchecked(PK id, TFunction<ENTITY, Boolean> mutator, EntityFactory<PK, ENTITY> factory) {
		try {
			return update(id, mutator, factory);
		} catch (TException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public OptResult<ENTITY> update(PK id, TFunction<ENTITY, Boolean> mutator) throws TException, E {
		return update(id, mutator, null);
	}
	
	protected Object[] extractCompaundPk(PK id){
		return new Object[]{id};
	}

	@Override
	public OptResult<ENTITY> update(PK id, TFunction<ENTITY, Boolean> mutator, EntityFactory<PK, ENTITY> factory) throws TException, E {
		
		try {
			return RwModelFactoryHelper.optimisticUpdate((count) -> {
				
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
	        		if (mapper.save(e, Option.ifNotExist())){
	        			invalidate(id);
	        			return OptResult.create(CassandraModelFactory.this, e, null, true);
	        		}else{
			        	if (count == 0)
			        		invalidate(id);
	        			return null;
	        		}
	        	}else{
	        		try{
	        			final boolean updated = mapper.update(e, orig, Option.onlyIf());
	        			
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
		} catch (EntityNotFoundException e) {
			throw createNotFoundException(id);
		}				
	}

	private OptResult<ENTITY> _delete(ENTITY e){				
		mapper.delete(e);
		invalidate((PK)e.getPk());
		return OptResult.create(this, null, e, true);
	}

	@Override
	public OptResult<ENTITY> delete(PK id) throws E {
		
		final ENTITY e = findEntityById(id);
		
		if (e == null)
			throw createNotFoundException(id);
		
		return _delete(e);
	}

	@Override
	public OptResult<ENTITY> optInsert(ENTITY e) {
		
		final long now = System.currentTimeMillis();
        
		if (e instanceof CreatedAtIF && (((CreatedAtIF)e).getCreatedAt() == 0))
        	((CreatedAtIF)e).setCreatedAt(now);
        
        if (e instanceof UpdatedAtIF)
        	((UpdatedAtIF) e).setUpdatedAt(now);

		final boolean saved = mapper.save(e, Option.ifNotExist());
		invalidate((PK)e.getPk());
				
		if (saved == false)
			throw new UniqueException(null, true, null);
		
		return OptResult.create(this, e, null, true);
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
	
	public Enumeration<PK> getAllIds(){
		
		final Select.Selection select = QueryBuilder.select();
		
		if (mapper.primaryKeySize() !=1)
			throw new RuntimeException("support only one pkey");

		final ColumnMapper<PK> cm = (ColumnMapper)mapper.getPrimaryKeyColumn(0);

		final ResultSet rs = mappingManager.getSession().execute(select.column(cm.getColumnName()).from(mapper.getTableMetadata().getName()).setFetchSize(1000));
		final Iterator<Row> it = rs.iterator();
		
		return new Enumeration<PK>(){

			@Override
			public boolean hasMoreElements() {
				return it.hasNext();
			}

			@Override
			public PK nextElement() {
				final Row row = it.next();
				final PK value;
	            final TypeCodec<Object> customCodec = cm.getCustomCodec();
	            if (customCodec != null)
	                value = (PK)row.get(0, customCodec);
	            else
	                value = (PK)row.get(0, cm.getJavaType());
				
				return value;
			}};
	}

}

package com.knockchat.cassandra.model;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.ArrayUtils;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

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
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.mapping.ColumnMapper;
import com.datastax.driver.mapping.EntityMapper.Scenario;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.Mapper.Option;
import com.datastax.driver.mapping.Mapper.UpdateQuery;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.NotModifiedException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.knockchat.appserver.model.AbstractCachedModelFactory;
import com.knockchat.appserver.model.DaoEntityIF;
import com.knockchat.appserver.model.LocalEventBus;
import com.knockchat.appserver.model.RwModelFactoryIF;
import com.knockchat.appserver.model.Unique;
import com.knockchat.appserver.model.UniqueException;
import com.knockchat.cassandra.DLock;
import com.knockchat.cassandra.DLockFactory;
import com.knockchat.utils.Pair;
import com.knockchat.utils.thrift.TFunction;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

public abstract class CassandraModelFactory<PK extends Serializable,ENTITY extends DaoEntityIF, E extends TException> extends AbstractCachedModelFactory<PK, ENTITY> implements RwModelFactoryIF<PK, ENTITY, E> {

	private final Class<ENTITY> entityClass;	
	private final Constructor<ENTITY> copyConstructor;

	@Autowired
	protected MappingManager mappingManager;
	
	@Autowired
	protected LocalEventBus localEventBus;
	
	protected Mapper<ENTITY> mapper;
	private DLockFactory dLockFactory;
	
	private final static Option noSaveNulls = Option.saveNullFields(false);
		
	private final List<Pair<ColumnMapper<ENTITY>, PreparedStatement>> uniqueColumns = Lists.newArrayList();
	
	protected abstract E createNotFoundException(PK id);
	
	//private volatile Map<String, PreparedStatement> preparedQueries = Collections.emptyMap();	
			
	public CassandraModelFactory(Cache cache, Class<ENTITY> entityClass) {
		super(cache);
		this.entityClass  = entityClass;
		try {
			copyConstructor = getEntityClass().getConstructor(getEntityClass());
		} catch (NoSuchMethodException | SecurityException e) {
			throw new RuntimeException(e);
		}				
	}
	
	public CassandraModelFactory(String cacheName, Class<ENTITY> entityClass) {
		super(cacheName);
		this.entityClass  = entityClass;
		try {
			copyConstructor = getEntityClass().getConstructor(getEntityClass());
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
	
	protected void initMapping(){
	
		dLockFactory  = new DLockFactory(mappingManager.getSession());
		mapper = mappingManager.newMapper(this.entityClass);
		
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
	}
	
	
	@Override
	public final Class<ENTITY> getEntityClass() {
		return entityClass;
	}
	
	protected Object[] extractCompaundPk(PK id){
		return new Object[]{id};
	}
	
//	private  Number fetchCurrentVersion(PK id, final Object[] pkeys, final ColumnMapper<ENTITY> versionColumn) throws EntityNotFoundException{
//		final Select.Where where = QueryBuilder.select(versionColumn.getColumnNameUnquoted()).from(mapper.getTableName()).where();
//		
//		for (int i=0; i< mapper.primaryKeySize(); i++){
//			where.and(QueryBuilder.eq(mapper.getPrimaryKeyColumn(i).getColumnNameUnquoted(), pkeys[i]));
//		}
//		where.setConsistencyLevel(ConsistencyLevel.SERIAL);
//		final ResultSet rs = getSession().execute(where);
//		final Row row = rs.one();
//		if (row == null)
//			throw new EntityNotFoundException(id);
//		
//        final TypeCodec<Object> customCodec = versionColumn.getCustomCodec();
//        if (customCodec != null)
//        	return (Number)row.get(0, customCodec);
//        else
//        	return (Number)row.get(0, versionColumn.getJavaType());					
//		
//	}
	
//	private static class KK{
//		private final Object[] kk;
//
//		private KK(Object[] kk) {
//			super();
//			this.kk = kk;
//		}
//		
//	    public static int hashCode(Object a[]) {
//	        if (a == null)
//	            return 0;
//
//	        int result = 1;
//
//	        for (int i=0; i<a.length-1; i++)
//	            result = 31 * result + (a[i] == null ? 0 : a[i].hashCode());
//
//	        return result;
//	    }
//
//	    private static boolean equals(Object[] a, Object[] a2) {
//	        if (a==a2)
//	            return true;
//	        if (a==null || a2==null)
//	            return false;
//
//	        if (a2.length != a.length)
//	            return false;
//
//	        for (int i=0; i<a.length-1; i++) {
//	            Object o1 = a[i];
//	            Object o2 = a2[i];
//	            if (!(o1==null ? o2==null : o1.equals(o2)))
//	                return false;
//	        }
//
//	        return true;
//	    }
//	    
//
//		@Override
//		public int hashCode() {
//			final int prime = 31;
//			int result = 1;
//			result = prime * result + hashCode(kk);
//			return result;
//		}
//
//		@Override
//		public boolean equals(Object obj) {
//			if (this == obj)
//				return true;
//			if (obj == null)
//				return false;
//			if (getClass() != obj.getClass())
//				return false;
//			KK other = (KK) obj;
//			if (!equals(kk, other.kk))
//				return false;
//			return true;
//		}		
//	}
	
//	private ListenableFuture<List<ResultSet>> fetchEntityIn(Collection<PK> ids, Scenario scenario) {
//		
//		final ArrayListMultimap<KK, Object> mm = ArrayListMultimap.create();
//		
//		for (PK id: ids){
//			final Object[] pk = extractCompaundPk(id);
//			mm.put(new KK(pk), pk[pk.length-1]);
//		}
//		
//		final List<ListenableFuture<ResultSet>> lfs = Lists.newArrayList();
//		
//		for (Map.Entry<KK, Collection<Object>> e : mm.asMap().entrySet()){
//			lfs.add(fetchEntityIn(e.getKey().kk, (List)e.getValue(), scenario));
//		}
//		
//		return Futures.allAsList(lfs);
//	}
//		
//	/**
//	 * select * from table where pk0= partKey[0] and pk1=partKey[1] and ... and cc in (clusterKeys)
//	 * @param partKey
//	 * @param clusterKeys
//	 * @param scenario
//	 * @return
//	 */
//	private ResultSetFuture fetchEntityIn(Object[] partKey, List<Object> clusterKeys, Scenario scenario) {
//		
//		//System.out.println("partKey=" + partKey + "");
//		
//		Select.Selection selection = QueryBuilder.select();
//		
//		for (ColumnMapper<ENTITY> cm : mapper.allColumns(scenario)){
//			selection = selection.column(cm.getColumnName());
//		}
//		
//		final Select select = selection.from(mapper.getTableName());
//		final Select.Where where = select.where();
//		
//		final List<ColumnMapper<ENTITY>> pks = Lists.newArrayList();
//
//		for (int i=0; i<mapper.primaryKeySize()-1; i++){
//			final ColumnMapper<ENTITY> cm = mapper.getPrimaryKeyColumn(i);
//			pks.add(cm);
//			where.and(QueryBuilder.eq(cm.getColumnName(), /*partKey[i]*/ QueryBuilder.bindMarker()));
//		}
//		
//		final ColumnMapper<ENTITY> kk = mapper.getPrimaryKeyColumn(mapper.primaryKeySize()-1); 
//		
//		where.and(QueryBuilder.in(kk.getColumnName(), /*clusterKeys*/ QueryBuilder.bindMarker()));		
//		//where.setConsistencyLevel(mapper.getReadConsistency());
//		
//		final PreparedStatement ps = getPreparedQuery(select.getQueryString());
//		
//		//System.out.println(ps.getQueryString());
//		final BoundStatement bs = ps.bind();
//			
//		int i;
//		for (i=0; i< pks.size(); i++){
//			final ColumnMapper<ENTITY> cm = pks.get(i);
//            final TypeCodec<Object> customCodec = cm.getCustomCodec();
//            if (customCodec != null)
//                bs.set(i, partKey[i], customCodec);
//            else
//                bs.set(i, partKey[i], cm.getJavaType());
//		}
//		
//        if (kk.getCustomCodec() !=null)
//        	throw new RuntimeException("custom codecs not supported here. Need for proper implementation");
//        
//        bs.setList(i, clusterKeys, kk.getJavaType());
//		bs.setConsistencyLevel(mapper.getReadConsistency());
//		
//		return getSession().executeAsync(bs);
//	}

	@Override
	final protected Map<PK, ENTITY> fetchEntityByIdAsMap(Collection<PK> _ids) {
		
		try {
			return fetchEntityByIdAsMapAsync(_ids).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}					
	}

	final protected ListenableFuture<Map<PK, ENTITY>> fetchEntityByIdAsMapAsync(Collection<PK> _ids) {
		
		final List<PK> ids = ImmutableList.copyOf(_ids);
				
		final List<ListenableFuture<ENTITY>> ff = ids.stream().map(pk -> (mapper.getAsync(extractCompaundPk(pk)))).collect(Collectors.toList());
		
		return Futures.transform(Futures.allAsList(ff), (List<ENTITY> ee) -> {
			
			final Map<PK, ENTITY> ret = Maps.newHashMap();
			for (int i=0; i< ids.size(); i++){
				ret.put(ids.get(i), ee.get(i));
			}
			return ret;			
		});
	}

	@Override
	final protected ENTITY fetchEntityById(PK id) {
		return mapper.get(extractCompaundPk(id));
	}
	
	public final <T> Iterator<T> fetchAll(String fieldName){
		
		final Select.Selection select = QueryBuilder.select();
		
		final ColumnMapper<ENTITY> cm = mapper.getColumnByFieldName(fieldName);
		if (cm == null)
			throw new RuntimeException("coundn't find mapper for property: " + fieldName);
		
		final ResultSet rs = mappingManager.getSession().execute(select.column(cm.getColumnNameUnquoted()).from(mapper.getTableMetadata().getName()).setFetchSize(1000));
		
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
	
	protected DLock assertUnique(ENTITY from, ENTITY to){
		
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
	
	public void putEntity(ENTITY entity) {
		putEntity(entity, true);
	}
	
	public void putEntity(ENTITY entity, boolean _noSaveNulls) {
		try {
			putEntityAsync(entity, _noSaveNulls).get();
		} catch (ExecutionException e) {
			throw Throwables.propagate(e.getCause());
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	/*
	 * save all not null fields without read
	 * Method not generates any events
	 */
	public ListenableFuture<Boolean> putEntityAsync(ENTITY entity, boolean _noSaveNulls) {
		if (entity.getPk() == null)
			throw new IllegalArgumentException("pk is null");
		
		final ListenableFuture<Boolean> f;
		if (_noSaveNulls)
			f = mapper.saveAsync(entity, noSaveNulls);
		else
			f = mapper.saveAsync(entity);
		
		Futures.addCallback(f, new FutureCallback<Boolean>(){

			@Override
			public void onSuccess(Boolean result) {
				invalidate((PK)entity.getPk());
			}

			@Override
			public void onFailure(Throwable t) {
				log.error("putEntityAsync", t);
			}});
		
		return f;				
	}
	
	public void putEntityAsync(ENTITY entity) {
		putEntityAsync(entity, true);
	}
	
	public void fetchAll(final int batchSize, Consumer<List<ENTITY>> consumer){
				
		final Iterator<ENTITY> r = mapper.getAll(Option.fetchSize(batchSize), Option.scenario(Scenario.ALL)).iterator();
			
		while(r.hasNext()){
			final List<ENTITY> batch = Lists.newArrayList(Iterators.limit(r, batchSize));
			consumer.accept(batch);
		}
    }
	
	@Override
	public String toString(){
		return mapper.toString();
	}

	@Override
	public ENTITY insertEntity(ENTITY e) throws UniqueException {
		putEntity(e, false);
		
    	localEventBus.postAsync(insertEntityEvent(e));		
		return e;
	}

	@Override
	public ENTITY updateEntity(ENTITY e) throws UniqueException {
		putEntity(e, true);
		
		localEventBus.postAsync(updateEntityEvent(null, e));
		
		return e;
	}
	
	@Override
	public void deleteEntity(ENTITY e) {
		mapper.delete(extractCompaundPk((PK)e.getPk()));
	}
	
	public ENTITY copy(ENTITY e){
		try {
			return copyConstructor.newInstance(e);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
			throw new RuntimeException(e1);
		}		
	}
	
	public Statement insertQuery(final ENTITY e, Option ... options){
		return mapper.saveQuery(e, options);
	}
	
	public Statement deleteQuery(final PK pk, Option ... options){
		return mapper.deleteQuery(ArrayUtils.addAll(extractCompaundPk(pk), options));
	}
	
	public UpdateQuery updateQuery(final ENTITY beforeUpdate, final ENTITY afterUpdate, TFunction<ENTITY, Boolean> mutator, Option... options) throws TException{
		
		if (!(mutator.apply(afterUpdate)))
			return null;		
		try{
			return mapper.updateQuery(beforeUpdate, afterUpdate, options);
		}catch(NotModifiedException e1){
			return null;
		}
	}
		
//	public final Statement customUpdateQuery(PK id, Assignment ...assignment) throws TException, E {
//	
//		if (id == null)
//			throw new IllegalArgumentException("id is null");
//	          	
//		final Update update = QueryBuilder.update(mapper.getTableName());
//		final Assignments assignments = update.with();
//		
//		for (Assignment a: assignment){
//			assignments.and(a);
//		}
//           	
//		final Update.Where uWhere = update.where();
//		for (int i=0; i< mapper.primaryKeySize(); i++){
//			uWhere.and(QueryBuilder.eq(mapper.getPrimaryKeyColumn(i).getColumnNameUnquoted(), QueryBuilder.bindMarker()));
//		}
//	
//		update.setConsistencyLevel(mapper.getWriteConsistency());
//		final String queryString = update.getQueryString();
//	
//		final BoundStatement bs = getPreparedQuery(queryString).bind();
//		
//		final Object[] pkeys = extractCompaundPk(id);
//
//		for (int i=0; i< mapper.primaryKeySize(); i++){
//			final ColumnMapper<ENTITY> cm = mapper.getPrimaryKeyColumn(i);
//			final TypeCodec<Object> customCodec = cm.getCustomCodec();
//			if (customCodec != null)
//				bs.set(i, pkeys[i], customCodec);
//			else
//				bs.set(i, pkeys[i], cm.getJavaType());
//		}
//		
//		return bs;
//	}
	
	public Constructor<ENTITY> getCopyConstructor() {
		return copyConstructor;
	}
	
	public Update update(){
		return QueryBuilder.update(mapper.getTableName());
	}
	
//    protected PreparedStatement getPreparedQuery(String queryString) {
//
//        PreparedStatement stmt = preparedQueries.get(queryString);
//        if (stmt == null) {
//            synchronized (preparedQueries) {
//                stmt = preparedQueries.get(queryString);
//                if (stmt == null){
//                    log.debug("Preparing query {}", queryString);
//                    stmt = getSession().prepare(queryString);
//                    final Map<String, PreparedStatement> newQueries = new HashMap<String, PreparedStatement>(preparedQueries);
//                    newQueries.put(queryString, stmt);
//                    preparedQueries = newQueries;
//                }
//            }
//        }
//        return stmt;
//    }
    
    final public ListenableFuture<Map<PK, ENTITY>> findEntityByIdAsMapAsync(Collection<PK> ids){
		if (CollectionUtils.isEmpty(ids))
			return Futures.immediateFuture(Collections.emptyMap());
		
		if (getCache() == null){
			return fetchEntityByIdAsMapAsync(ids);
		}
		
		final Map<Object, Element> cached = getCache().getAll(ids);
		final Map<PK, ENTITY> ret = Maps.newHashMap();
		final List<PK> keysToLoad = Lists.newArrayList();
		
		for (Map.Entry<Object, Element> e: cached.entrySet()){
			if (e.getValue() !=null){
				ret.put((PK)e.getKey(), (ENTITY)e.getValue().getObjectValue());
			}else{
				keysToLoad.add((PK)e.getKey());	
			}
		}
		
		final List<ListenableFuture<ENTITY>> ff = keysToLoad.parallelStream().map(pk -> (mapper.getAsync(extractCompaundPk(pk)))).collect(Collectors.toList());
						
		return Futures.transform(Futures.allAsList(ff), (List<ENTITY> ee) -> {
			
			for (int i=0; i< keysToLoad.size(); i++){
				final PK key = keysToLoad.get(i); 
				final ENTITY value = ee.get(i);
				final Element toPut = new Element(key, value);
				getCache().put(toPut);
				
				if (getCache().getCacheConfiguration().isCopyOnWrite()){
					ret.put(key, value);
				}else if (getCache().getCacheConfiguration().isCopyOnRead()){
					final Element copy = getCache().getCacheConfiguration().getCopyStrategy().copyForRead(toPut, getClass().getClassLoader());
					ret.put(key, (ENTITY)copy.getObjectValue());
				}else{
					ret.put(key, value);					
				}								
			}
			return ret;			
		});		
    }

}

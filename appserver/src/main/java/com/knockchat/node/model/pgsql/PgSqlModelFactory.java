package com.knockchat.node.model.pgsql;

import java.io.Serializable;

import org.apache.thrift.TException;
import org.hibernate.SessionFactory;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.node.model.LocalEventBus;
import com.knockchat.node.model.UniqueException;
import com.knockchat.utils.Pair;

import net.sf.ehcache.Cache;

public class PgSqlModelFactory<PK extends Serializable, ENTITY extends DaoEntityIF> extends AbstractPgSqlModelFactory<PK, ENTITY, TException>{

	public PgSqlModelFactory(String cacheName, Class<ENTITY> entityClass) {
		super(cacheName, entityClass);
	}

	public PgSqlModelFactory(Cache cache, Class<ENTITY> entityClass, ListeningExecutorService listeningExecutorService, SessionFactory sessionFactory, LocalEventBus localEventBus) {
		super(cache, entityClass, listeningExecutorService, sessionFactory, localEventBus);
	}
	
	@Override
	public final ENTITY insertEntity(ENTITY e) throws UniqueException {
		final ENTITY ret = getDao().save(e).first;
		_invalidateEhCache((PK)ret.getPk());
		
    	localEventBus.post(syncInsertEntityEvent(ret));
    	localEventBus.postAsync(asyncInsertEntityEvent(ret));
		
		return ret;
	}

	@Override
	@Transactional
	public final ENTITY updateEntity(ENTITY e) throws UniqueException {		
		final ENTITY before;
		if (e.getPk()!=null){
			before = getDao().findById((PK)e.getPk());
		}else{
			before = null;
		}

		final Pair<ENTITY, Boolean> r = getDao().saveOrUpdate(e);
		_invalidateEhCache((PK)r.first.getPk());

		if (r.second){
	    	localEventBus.post(syncUpdateEntityEvent(before, r.first));
	    	localEventBus.postAsync(asyncUpdateEntityEvent(before, r.first));			
		}
		return r.first;
	}

	@Override
	protected final TException createNotFoundException(PK id) {
		return new TException("Entity " + id + "not found");
	}

}

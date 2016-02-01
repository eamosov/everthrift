package com.knockchat.node.model.pgsql;

import java.io.Serializable;
import java.util.List;

import org.hibernate.SessionFactory;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.node.model.ModelFactoryIF;

import net.sf.ehcache.Cache;

public class PgSqlModelFactory<PK extends Serializable, ENTITY extends DaoEntityIF> extends AbstractPgSqlModelFactory<PK, ENTITY> implements ModelFactoryIF<PK, ENTITY>{

	public PgSqlModelFactory(String cacheName, Class<ENTITY> entityClass) {
		super(cacheName, entityClass);
	}

	public PgSqlModelFactory(Cache cache, Class<ENTITY> entityClass, ListeningExecutorService listeningExecutorService, List<SessionFactory> sessionFactories) {
		super(cache, entityClass, listeningExecutorService, sessionFactories);
	}

	@Override
	public ENTITY update(ENTITY e) {
		return super.helper.updateEntity(e);
	}
	
	@Override
	public boolean isUpdated() {		
		return helper.isUpdated();
	}
}

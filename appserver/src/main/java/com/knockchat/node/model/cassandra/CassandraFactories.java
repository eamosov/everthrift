package com.knockchat.node.model.cassandra;

import java.util.Map;

import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.Session;
import com.google.common.collect.Maps;
import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.utils.PosAppInitializingBean;
import com.knockchat.utils.thrift.TFunction;
import com.knockchat.utils.thrift.TVoidFunction;

@SuppressWarnings("rawtypes")
@Component
public class CassandraFactories implements PosAppInitializingBean{
	
	@Autowired
	private ApplicationContext ctx;
	
	@Autowired(required=false)
	private Session session;
	
	private Map<Class, CassandraModelFactory> factories = Maps.newHashMap();
	
	public synchronized void register(CassandraModelFactory f){
		factories.put(f.getEntityClass(), f);
	}
	
	public synchronized <ENTITY extends DaoEntityIF> CassandraModelFactory<?,ENTITY,?> of(Class<ENTITY> cls){
		final CassandraModelFactory<?,ENTITY,?> f =  factories.get(cls);
		
		if (f == null)
			throw new RuntimeException("Cound't find factory for " + cls.getCanonicalName());
		
		return f;
	}

	public Statements begin(){
		return new Statements(this, session);
	}

	public Session getSession() {
		return session;
	}

	public void setSession(Session session) {
		this.session = session;
	}
	
	public void batch(TVoidFunction<Statements> run) throws TException{
		Statements s = begin();
		run.apply(s);
		s.commit();
	}
	
	public <E> E batch(TFunction<Statements, E> run) throws TException{
		Statements s = begin();		
		final E ret = run.apply(s); 
		s.commit();
		return ret;
	}

	public void execute(TVoidFunction<Statements> run) throws TException{
		Statements s = begin().setBatch(false);
		run.apply(s);
		s.commit();
	}
	
	public <E> E execute(TFunction<Statements, E> run) throws TException{
		Statements s = begin().setBatch(false);		
		final E ret = run.apply(s); 
		s.commit();
		return ret;
	}

	@Override
	public void afterAppInitizlized() throws Exception {
		for (CassandraModelFactory f: ctx.getBeansOfType(CassandraModelFactory.class).values())
			register(f);		
	}
	
}

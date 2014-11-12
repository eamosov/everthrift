package com.knockchat.node.model;

import java.io.Serializable;
import java.util.List;
import java.util.SortedSet;

import org.hibernate.SessionFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.knockchat.hibernate.dao.AbstractDao;
import com.knockchat.hibernate.dao.AbstractDaoImpl;
import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.sql.objects.ObjectStatements;

public abstract class AbstractModelFactory<PK extends Serializable, ENTITY extends DaoEntityIF<ENTITY>> implements InitializingBean {

    @Autowired
    protected ObjectStatements objectStatements;

    @Autowired
    protected List<SessionFactory> sessionFactories;
    private final Class<ENTITY> entityClass;

    @Autowired
    protected ListeningExecutorService listeningExecutorService;

    protected AbstractDao<PK, ENTITY> dao;

    protected AbstractModelFactory() {
        this(null);
    }

    protected AbstractModelFactory(Class<ENTITY> entityClass) {
        this.entityClass = entityClass;
        if (this.entityClass == null)
            dao = null;

        else
            dao = new AbstractDaoImpl(this.entityClass);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (dao != null) {
        	
        	SessionFactory sf = null;
        	
            for (SessionFactory factory : sessionFactories)
                if (factory.getClassMetadata(this.entityClass) != null){
                	sf = factory;
                	break;
                }

            if (sf == null)
            	throw new RuntimeException("Cound't find SessionFactory for class " + this.entityClass.getSimpleName());
            	            
            dao.setSessionFactory(sf);            
            dao.setListeningExecutorService(listeningExecutorService);
        }
    }

    public ENTITY update(ENTITY e) {
        return dao.saveOrUpdate(e);
    }
    
    public AbstractDao<PK, ENTITY> getDao(){
    	return dao;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void update(List<ENTITY> l) {
        for (ENTITY e : (SortedSet<ENTITY>) (SortedSet) Sets.newTreeSet((List<Comparable<?>>) l))
            update(e);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void update(ENTITY[] l) {
        final SortedSet<ENTITY> s = (SortedSet<ENTITY>) (SortedSet) Sets.newTreeSet();
        for (ENTITY e : l)
            s.add(e);

        for (ENTITY e : l)
            update(e);
    }

}

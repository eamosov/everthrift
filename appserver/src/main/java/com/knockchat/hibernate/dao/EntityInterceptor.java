package com.knockchat.hibernate.dao;

import java.io.Serializable;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.hibernate.EmptyInterceptor;
import org.hibernate.Transaction;
import org.hibernate.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.knockchat.appserver.model.UpdatedAtIF;

public class EntityInterceptor extends EmptyInterceptor {
	
	private final static Logger log = LoggerFactory.getLogger(EntityInterceptor.class);

	private static final long serialVersionUID = 1L;
	
	public static final EntityInterceptor INSTANCE = new EntityInterceptor();
	
	private final ThreadLocal<Set<Object>> dirty = new ThreadLocal<Set<Object>>(){
		protected Set<Object> initialValue(){
			return Sets.newIdentityHashSet();
		}
	};
		
	private EntityInterceptor(){
		
	}
	
	public void afterTransactionBegin(Transaction tx) {
		log.debug("begin transaction");
		dirty.get().clear();
	}
	
	public void afterTransactionCompletion(Transaction tx) {
		log.debug("end transaction");
		dirty.get().clear();
	}
	
	public boolean isDirty(Object e){
		return dirty.get().contains(e);
	}

	@Override
	public boolean onFlushDirty(
			Object entity, 
			Serializable id, 
			Object[] currentState, 
			Object[] previousState, 
			String[] propertyNames, 
			Type[] types) {
		
		if (log.isDebugEnabled())
			log.debug("onFlushDirty, class={}, object={}, id={}", entity.getClass().getSimpleName(), System.identityHashCode(entity), id);
		
		dirty.get().add(entity);
						
		boolean updated = false;
				
		if (entity instanceof UpdatedAtIF){
			
			final int idx = ArrayUtils.indexOf(propertyNames, "updatedAt");
			if (idx>0){
				final Long now = Long.valueOf(System.currentTimeMillis()/1000);
				if (!now.equals(currentState[idx])){
					currentState[idx] = now;
					updated = true;
				}else{
					updated = false;
				}				
			}						 
		}
		
		return updated;
	}

}

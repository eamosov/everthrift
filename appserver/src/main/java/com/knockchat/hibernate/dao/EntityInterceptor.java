package com.knockchat.hibernate.dao;

import java.io.Serializable;

import org.apache.commons.lang.ArrayUtils;
import org.hibernate.EmptyInterceptor;
import org.hibernate.type.Type;

import com.knockchat.appserver.model.UpdatedAtIF;

public class EntityInterceptor extends EmptyInterceptor {

	private static final long serialVersionUID = 1L;
	

	@Override
	public boolean onFlushDirty(
			Object entity, 
			Serializable id, 
			Object[] currentState, 
			Object[] previousState, 
			String[] propertyNames, 
			Type[] types) {
						
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

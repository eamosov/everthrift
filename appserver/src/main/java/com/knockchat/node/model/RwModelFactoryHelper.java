package com.knockchat.node.model;

import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.thrift.TException;

import com.google.common.base.Throwables;
import com.knockchat.appserver.model.CreatedAtIF;
import com.knockchat.appserver.model.UpdatedAtIF;
import com.knockchat.node.model.pgsql.OptimisticUpdateFailException;

public abstract class RwModelFactoryHelper<PK, ENTITY> {
	
    protected abstract ENTITY updateEntityImpl(ENTITY e);
    protected abstract PK extractPk(ENTITY e);
	        
    private ThreadLocal<Boolean> isUpdated = new ThreadLocal<Boolean>(){
    	@Override
        protected Boolean initialValue() {
            return false;
        }
    }; 

    public RwModelFactoryHelper() {

    }
    
    public boolean isUpdated(){
    	return isUpdated.get();
    }
    
    public void setUpdated(boolean value){
    	isUpdated.set(value);
    }            
    
    public ENTITY updateEntity(ENTITY e) {
    	
    	final long now = System.currentTimeMillis() / 1000;
    	        
    	if (extractPk(e) == null){

            if (e instanceof CreatedAtIF)
            	((CreatedAtIF)e).setCreatedAt(now);
            
            if (e instanceof UpdatedAtIF)
            	((UpdatedAtIF) e).setUpdatedAt(now);

    	}
    	
    	return updateEntityImpl(e);
    }
    
    public static <T> T optimisticUpdate(Callable<T> updateFunction) throws OptimisticUpdateFailException, EntityNotFoundException, TException{
    	return optimisticUpdate(updateFunction, 5, 100);
    }
    
    public static <T> T optimisticUpdate(Callable<T> updateFunction, int maxIteration, int maxTimeoutMillis) throws OptimisticUpdateFailException, EntityNotFoundException, TException{
			int i=0;
			T updated = null;
			do{
				try {
					updated = updateFunction.call();
				} catch (TException | EntityNotFoundException e1) {
					throw e1;
				} catch (Exception e1){
					throw Throwables.propagate(e1);
				}
				
				i++;
				if (updated == null)
					try {
						Thread.sleep(new Random().nextInt(maxTimeoutMillis));
					} catch (InterruptedException e) {
					}					
			}while(updated == null && i<maxIteration);
			
			if (updated == null)
				throw new OptimisticUpdateFailException();
			
			return updated;
    }
    
}

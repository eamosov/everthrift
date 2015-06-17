package com.knockchat.node.model;

import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.thrift.TException;

import com.google.common.base.Throwables;
import com.knockchat.appserver.model.CreatedAtIF;
import com.knockchat.appserver.model.UpdatedAtIF;
import com.knockchat.node.model.pgsql.OptimisticUpdateFailException;
import com.knockchat.utils.LongTimestamp;

public abstract class RwModelFactoryHelper<PK, ENTITY> {
	
	public static final int MAX_ITERATIONS = 20;
	public static final int MAX_TIMEOUT = 100;
	
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
    	
    	final long now = LongTimestamp.now();
    	        
    	if (extractPk(e) == null){

            if (e instanceof CreatedAtIF)
            	((CreatedAtIF)e).setCreatedAt(now);
            
            if (e instanceof UpdatedAtIF)
            	((UpdatedAtIF) e).setUpdatedAt(now);

    	}
    	
    	return updateEntityImpl(e);
    }
    
    public static <T> T optimisticUpdate(Callable<T> updateFunction) throws OptimisticUpdateFailException, EntityNotFoundException, TException{
    	return optimisticUpdate(updateFunction, MAX_ITERATIONS, MAX_TIMEOUT);
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

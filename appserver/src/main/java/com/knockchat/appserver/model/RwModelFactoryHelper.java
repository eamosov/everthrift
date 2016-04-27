package com.knockchat.appserver.model;

//public abstract class RwModelFactoryHelper<PK, ENTITY> {
//	
//	public static final int MAX_ITERATIONS = 20;
//	public static final int MAX_TIMEOUT = 100;
//	
//	public static interface UpdateFunction<T>{
//		T apply(int count) throws TException, EntityNotFoundException;
//	}
//	
//    protected abstract ENTITY updateEntityImpl(ENTITY e);
//    protected abstract PK extractPk(ENTITY e);
//	        
//    private ThreadLocal<Boolean> isUpdated = new ThreadLocal<Boolean>(){
//    	@Override
//        protected Boolean initialValue() {
//            return false;
//        }
//    }; 
//
//    public RwModelFactoryHelper() {
//
//    }
//    
//    public boolean isUpdated(){
//    	return isUpdated.get();
//    }
//    
//    public void setUpdated(boolean value){
//    	isUpdated.set(value);
//    }            
//    
//    public ENTITY updateEntity(ENTITY e) {
//    	
//    	final long now = LongTimestamp.now();
//    	        
//    	if (extractPk(e) == null){
//
//            if (e instanceof CreatedAtIF)
//            	((CreatedAtIF)e).setCreatedAt(now);
//            
//            if (e instanceof UpdatedAtIF)
//            	((UpdatedAtIF) e).setUpdatedAt(now);
//
//    	}
//    	
//    	return updateEntityImpl(e);
//    }
//    
//    public static <T> T optimisticUpdate(UpdateFunction<T> updateFunction) throws OptimisticUpdateFailException, EntityNotFoundException, TException{
//    	return optimisticUpdate(updateFunction, MAX_ITERATIONS, MAX_TIMEOUT);
//    }
//    
//    public static <T> T optimisticUpdate(UpdateFunction<T> updateFunction, int maxIteration, int maxTimeoutMillis) throws OptimisticUpdateFailException, EntityNotFoundException, TException{
//			int i=0;
//			T updated = null;
//			do{
//				updated = updateFunction.apply(i);
//				
//				i++;
//				if (updated == null)
//					try {
//						Thread.sleep(new Random().nextInt(maxTimeoutMillis));
//					} catch (InterruptedException e) {
//					}					
//			}while(updated == null && i<maxIteration);
//			
//			if (updated == null)
//				throw new OptimisticUpdateFailException();
//			
//			return updated;
//    }
//    
//}

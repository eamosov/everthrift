package org.everthrift.appserver.model;

public interface CreatedAtIF {
    void setCreatedAt(long value);
    long getCreatedAt();
    
    public static void setCreatedAt(Object e){
        final long now = System.currentTimeMillis();
        
        if (e instanceof CreatedAtIF && (((CreatedAtIF)e).getCreatedAt() == 0))
            ((CreatedAtIF)e).setCreatedAt(now);

        if (e instanceof UpdatedAtIF)
            ((UpdatedAtIF) e).setUpdatedAt(now);
        
    }
    
    public static void setUpdatedAt(Object e){
        if (e instanceof UpdatedAtIF)
            ((UpdatedAtIF) e).setUpdatedAt(System.currentTimeMillis());        
    }
    
}

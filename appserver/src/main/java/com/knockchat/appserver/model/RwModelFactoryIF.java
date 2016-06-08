package com.knockchat.appserver.model;

import org.apache.thrift.TException;

import com.knockchat.appserver.model.events.DeleteEntityEvent;
import com.knockchat.appserver.model.events.InsertEntityEvent;
import com.knockchat.appserver.model.events.UpdateEntityEvent;

public interface RwModelFactoryIF<PK, ENTITY extends DaoEntityIF, E extends TException> extends RoModelFactoryIF<PK, ENTITY>{		            
        
    ENTITY insertEntity(ENTITY e) throws UniqueException;
    
    ENTITY updateEntity(ENTITY e) throws UniqueException;
            
    void deleteEntity(ENTITY e) throws E;
        
	default InsertEntityEvent<PK, ENTITY> insertEntityEvent(ENTITY entity){
		return new InsertEntityEvent<PK, ENTITY>(this, entity);
	}

	default UpdateEntityEvent<PK, ENTITY> updateEntityEvent(ENTITY before, ENTITY after){
		return new UpdateEntityEvent<PK, ENTITY>(this, before, after);
	}

	default DeleteEntityEvent<PK, ENTITY> deleteEntityEvent(ENTITY entity){
		return new DeleteEntityEvent<PK, ENTITY>(this, entity);
	}
    
}

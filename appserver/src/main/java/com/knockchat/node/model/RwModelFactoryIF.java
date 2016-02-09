package com.knockchat.node.model;

import org.apache.thrift.TException;

import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.node.model.events.DeleteEntityEvent.AsyncDeleteEntityEvent;
import com.knockchat.node.model.events.DeleteEntityEvent.SyncDeleteEntityEvent;
import com.knockchat.node.model.events.InsertEntityEvent.AsyncInsertEntityEvent;
import com.knockchat.node.model.events.InsertEntityEvent.SyncInsertEntityEvent;
import com.knockchat.node.model.events.UpdateEntityEvent.AsyncUpdateEntityEvent;
import com.knockchat.node.model.events.UpdateEntityEvent.SyncUpdateEntityEvent;

public interface RwModelFactoryIF<PK, ENTITY extends DaoEntityIF, E extends TException> extends RoModelFactoryIF<PK, ENTITY>{		            
        
    ENTITY insertEntity(ENTITY e) throws UniqueException;
    
    ENTITY updateEntity(ENTITY e) throws UniqueException;
            
    void deleteEntity(ENTITY e) throws E;
        
	default SyncInsertEntityEvent<PK, ENTITY> syncInsertEntityEvent(ENTITY entity){
		return new SyncInsertEntityEvent<PK, ENTITY>(this, entity);
	}

	default AsyncInsertEntityEvent<PK, ENTITY> asyncInsertEntityEvent(ENTITY entity){
		return new AsyncInsertEntityEvent<PK, ENTITY>(this, entity);
	}

	default SyncUpdateEntityEvent<PK, ENTITY> syncUpdateEntityEvent(ENTITY before, ENTITY after){
		return new SyncUpdateEntityEvent<PK, ENTITY>(this, before, after);
	}

	default AsyncUpdateEntityEvent<PK, ENTITY> asyncUpdateEntityEvent(ENTITY before, ENTITY after){
		return new AsyncUpdateEntityEvent<PK, ENTITY>(this, before, after);
	}

	default SyncDeleteEntityEvent<PK, ENTITY> syncDeleteEntityEvent(ENTITY entity){
		return new SyncDeleteEntityEvent<PK, ENTITY>(this, entity);
	}

	default AsyncDeleteEntityEvent<PK, ENTITY> asyncDeleteEntityEvent(ENTITY entity){
		return new AsyncDeleteEntityEvent<PK, ENTITY>(this, entity);
	}
    
}

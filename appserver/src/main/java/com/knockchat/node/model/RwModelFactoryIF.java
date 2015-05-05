package com.knockchat.node.model;


public interface RwModelFactoryIF<PK, ENTITY> extends RoModelFactoryIF<PK, ENTITY>{		            
        
    ENTITY insert(ENTITY e);
            
    void deleteEntity(ENTITY e);
        	
	/**
	 * Метод вызывается после обновления entity и служит для сброса кешей и реиндексирования связанный с ним сущностей
	 * @param entity
	 */
	public void updateRelatedData(ENTITY entity);		
}

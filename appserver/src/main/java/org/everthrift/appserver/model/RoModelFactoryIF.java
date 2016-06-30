package org.everthrift.appserver.model;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface RoModelFactoryIF<PK, ENTITY>{

    Class<ENTITY> getEntityClass();

    ENTITY findEntityById(PK id);

    /**
     * 	Если entity не найдено, то метод возвращает null для этого ключа
     * 	@param ids
     * 	@return
     */
    Map<PK, ENTITY> findEntityByIdAsMap(Collection<PK> ids);

    Collection<ENTITY> findEntityById(Collection<PK> ids);

    List<ENTITY> findEntityByIdsInOrder(Collection<PK> ids);

    Map<List<PK>, List<ENTITY>> findEntityByCollectionIds(Collection<List<PK>> listCollection);
}

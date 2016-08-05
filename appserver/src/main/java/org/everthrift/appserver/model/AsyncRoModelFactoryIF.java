package org.everthrift.appserver.model;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.util.concurrent.ListenableFuture;

public interface AsyncRoModelFactoryIF<PK, ENTITY> {
    
    ListenableFuture<ENTITY> findEntityByIdAsync(PK id);

    ListenableFuture<Map<PK, ENTITY>> findEntityByIdAsMapAsync(Collection<PK> ids);

    ListenableFuture<List<ENTITY>> findEntityByIdsInOrderAsync(final Collection<PK> ids);
}

package org.everthrift.appserver.model;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface AsyncRoModelFactoryIF<PK, ENTITY> {

    CompletableFuture<ENTITY> findEntityByIdAsync(PK id);

    CompletableFuture<Map<PK, ENTITY>> findEntityByIdAsMapAsync(Collection<PK> ids);

    CompletableFuture<List<ENTITY>> findEntityByIdsInOrderAsync(final Collection<PK> ids);
}

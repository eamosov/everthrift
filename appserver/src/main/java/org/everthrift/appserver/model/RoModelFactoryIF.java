package org.everthrift.appserver.model;


import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface RoModelFactoryIF<PK, ENTITY, E extends Exception> {

    @NotNull
    Class<ENTITY> getEntityClass();

    @Nullable
    ENTITY findEntityById(@NotNull PK id);

    @NotNull
    default ENTITY findEntityByIdOrThrow(@NotNull PK id) throws E {
        return Optional.ofNullable(findEntityById(id)).orElseThrow(() -> (createNotFoundException(id)));
    }

    @NotNull
    E createNotFoundException(@Nullable PK id);

    /**
     * Если entity не найдено, то метод возвращает null для этого ключа
     *
     * @param ids
     * @return
     */
    @NotNull
    Map<PK, ENTITY> findEntityByIdAsMap(@NotNull Collection<PK> ids);

    @NotNull
    Collection<ENTITY> findEntityById(@NotNull Collection<PK> ids);

    @NotNull
    List<ENTITY> findEntityByIdsInOrder(@NotNull Collection<PK> ids);

    @NotNull
    Map<List<PK>, List<ENTITY>> findEntityByCollectionIds(@NotNull Collection<List<PK>> listCollection);
}

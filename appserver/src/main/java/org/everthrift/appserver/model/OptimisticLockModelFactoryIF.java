package org.everthrift.appserver.model;

import com.google.common.base.Throwables;
import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TException;
import org.everthrift.appserver.model.pgsql.OptimisticUpdateFailException;
import org.everthrift.thrift.TFunction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.Random;

public interface OptimisticLockModelFactoryIF<PK extends Serializable, ENTITY extends DaoEntityIF, E extends TException>
    extends RwModelFactoryIF<PK, ENTITY, E> {

    @NotNull
    OptResult<ENTITY> updateUnchecked(@NotNull PK id, @NotNull TFunction<ENTITY, Boolean> mutator);

    @NotNull
    OptResult<ENTITY> updateUnchecked(@NotNull PK id, @NotNull TFunction<ENTITY, Boolean> mutator, @NotNull final EntityFactory<PK, ENTITY> factory);

    @NotNull
    OptResult<ENTITY> update(@NotNull PK id, @NotNull TFunction<ENTITY, Boolean> mutator) throws TException, UniqueException, E;

    @NotNull
    OptResult<ENTITY> update(@NotNull PK id, @NotNull TFunction<ENTITY, Boolean> mutator,
                             @NotNull final EntityFactory<PK, ENTITY> factory) throws TException, UniqueException, E;

    @NotNull
    OptResult<ENTITY> delete(@NotNull final PK id) throws E;

    @NotNull
    OptResult<ENTITY> optInsert(@NotNull final ENTITY e) throws UniqueException;

    int MAX_ITERATIONS = 20;

    int MAX_TIMEOUT = 100;

    interface UpdateFunction<T> {
        @Nullable
        T apply(int count) throws TException, EntityNotFoundException;
    }

    @NotNull
    static <T> T optimisticUpdate(@NotNull UpdateFunction<T> updateFunction) throws OptimisticUpdateFailException, EntityNotFoundException, TException {
        return optimisticUpdate(updateFunction, MAX_ITERATIONS, MAX_TIMEOUT);
    }

    @NotNull
    static <T> T optimisticUpdate(@NotNull UpdateFunction<T> updateFunction, int maxIteration,
                                  int maxTimeoutMillis) throws OptimisticUpdateFailException, EntityNotFoundException, TException {
        int i = 0;
        T updated = null;
        do {
            updated = updateFunction.apply(i);

            i++;
            if (updated == null) {
                try {
                    Thread.sleep(new Random().nextInt(maxTimeoutMillis));
                } catch (InterruptedException e) {
                }
            }
        } while (updated == null && i < maxIteration);

        if (updated == null) {
            throw new OptimisticUpdateFailException();
        }

        return updated;
    }

    @Override
    @NotNull
    default ENTITY updateEntity(@NotNull ENTITY e) throws UniqueException {
        throw new NotImplementedException();
    }

    @Override
    @NotNull
    default ENTITY insertEntity(@NotNull ENTITY e) throws UniqueException {
        return this.optInsert(e).afterUpdate;
    }

    @Override
    @NotNull
    default void deleteEntity(@NotNull ENTITY e) throws E {
        delete((PK)e.getPk());
    }
}

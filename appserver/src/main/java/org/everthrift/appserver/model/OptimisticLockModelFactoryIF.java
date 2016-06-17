package org.everthrift.appserver.model;

import java.util.Random;

import org.apache.thrift.TException;
import org.everthrift.appserver.model.pgsql.OptimisticUpdateFailException;
import org.everthrift.thrift.TFunction;

public interface OptimisticLockModelFactoryIF<PK, ENTITY extends DaoEntityIF, E extends TException> extends RwModelFactoryIF<PK, ENTITY, E> {

	OptResult<ENTITY> updateUnchecked(PK id, TFunction<ENTITY, Boolean> mutator);

	OptResult<ENTITY> updateUnchecked(PK id, TFunction<ENTITY, Boolean> mutator, final EntityFactory<PK, ENTITY> factory);

	OptResult<ENTITY> update(PK id, TFunction<ENTITY, Boolean> mutator) throws TException, UniqueException, E;

	OptResult<ENTITY> update(PK id, TFunction<ENTITY, Boolean> mutator, final EntityFactory<PK, ENTITY> factory) throws TException, UniqueException, E;

	OptResult<ENTITY> delete(final PK id) throws E;

	OptResult<ENTITY> optInsert(final ENTITY e) throws UniqueException;
	
	public static final int MAX_ITERATIONS = 20;
	public static final int MAX_TIMEOUT = 100;


	public static interface UpdateFunction<T> {
		T apply(int count) throws TException, EntityNotFoundException;
	}

	public static <T> T optimisticUpdate(UpdateFunction<T> updateFunction) throws OptimisticUpdateFailException, EntityNotFoundException, TException{
		return optimisticUpdate(updateFunction, MAX_ITERATIONS, MAX_TIMEOUT);
	}
	
	public static <T> T optimisticUpdate(UpdateFunction<T> updateFunction, int maxIteration, int maxTimeoutMillis)
			throws OptimisticUpdateFailException, EntityNotFoundException, TException {
		int i = 0;
		T updated = null;
		do {
			updated = updateFunction.apply(i);

			i++;
			if (updated == null)
				try {
					Thread.sleep(new Random().nextInt(maxTimeoutMillis));
				} catch (InterruptedException e) {
				}
		} while (updated == null && i < maxIteration);

		if (updated == null)
			throw new OptimisticUpdateFailException();

		return updated;
	}
}

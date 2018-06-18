package org.everthrift.sql.pgsql;

import org.apache.thrift.TException;
import org.everthrift.appserver.model.DaoEntityIF;
import org.everthrift.appserver.model.UniqueException;
import org.everthrift.utils.Pair;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.infinispan.Cache;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

public class PgSqlModelFactory<PK extends Serializable, ENTITY extends DaoEntityIF, E extends TException>
    extends AbstractPgSqlModelFactory<PK, ENTITY, E> {

    public PgSqlModelFactory(Cache<PK, ENTITY> cache, Class<ENTITY> entityClass, boolean copyOnRead) {
        super(cache, entityClass, copyOnRead);
    }

    @NotNull
    @Override
    public final ENTITY insertEntity(@NotNull ENTITY e) throws UniqueException {
        final ENTITY ret = getDao().save(e).first;
        _invalidateJCache((PK) ret.getPk(), InvalidateCause.INSERT);

        localEventBus.postEntityEvent(insertEntityEvent(ret));

        return ret;
    }

    @NotNull
    @Override
    public final ENTITY updateEntity(@NotNull ENTITY e) throws UniqueException {
        final ENTITY before;

        final Session session = getDao().getCurrentSession();
        Transaction tx = session.beginTransaction();

        try {

            if (e.getPk() != null) {
                before = getDao().findById((PK) e.getPk());
            } else {
                before = null;
            }

            final Pair<ENTITY, Boolean> r = getDao().saveOrUpdate(e);
            tx.commit();

            _invalidateJCache((PK) r.first.getPk(), before != null ? InvalidateCause.UPDATE : InvalidateCause.INSERT);

            if (r.second) {
                localEventBus.postEntityEvent(updateEntityEvent(before, r.first));
            }
            return r.first;
        } catch (Exception ex) {
            tx.rollback();
            throw ex;
        }
    }

    @Override
    public final void deleteEntity(@NotNull ENTITY e) throws E {
        final PK pk = (PK) e.getPk();
        final ENTITY _e = fetchEntityById(pk);
        if (_e == null) {
            throw createNotFoundException(pk);
        }

        dao.delete(_e);
        _invalidateJCache(pk, InvalidateCause.DELETE);

        localEventBus.postEntityEvent(deleteEntityEvent(_e));
    }

}

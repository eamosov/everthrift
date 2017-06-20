package org.everthrift.sql.pgsql;

import com.google.common.util.concurrent.ListeningExecutorService;
import net.sf.ehcache.Cache;
import org.apache.thrift.TException;
import org.everthrift.appserver.model.DaoEntityIF;
import org.everthrift.appserver.model.LocalEventBus;
import org.everthrift.appserver.model.UniqueException;
import org.everthrift.utils.Pair;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.springframework.beans.factory.annotation.Qualifier;

import java.io.Serializable;

public class PgSqlModelFactory<PK extends Serializable, ENTITY extends DaoEntityIF, E extends TException>
    extends AbstractPgSqlModelFactory<PK, ENTITY, E> {

    public PgSqlModelFactory(Cache cache, Class<ENTITY> entityClass,
                             @Qualifier("listeningCallerRunsBoundQueueExecutor") ListeningExecutorService listeningExecutorService,
                             SessionFactory sessionFactory,
                             LocalEventBus localEventBus) {
        super(cache, entityClass, listeningExecutorService, sessionFactory, localEventBus);
    }

    @Override
    public final ENTITY insertEntity(ENTITY e) throws UniqueException {
        final ENTITY ret = getDao().save(e).first;
        _invalidateEhCache((PK) ret.getPk());

        localEventBus.postEntityEvent(insertEntityEvent(ret));

        return ret;
    }

    @Override
    public final ENTITY updateEntity(ENTITY e, ENTITY old) throws UniqueException {
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

            _invalidateEhCache((PK) r.first.getPk());

            if (r.second) {
                localEventBus.postEntityEvent(updateEntityEvent(before, r.first));
            }
            return r.first;
        } catch (Exception ex) {
            tx.rollback();
            throw ex;
        }
    }

}

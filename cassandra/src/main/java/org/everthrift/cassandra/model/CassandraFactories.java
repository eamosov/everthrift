package org.everthrift.cassandra.model;

import com.datastax.driver.core.Session;
import com.google.common.collect.Maps;
import org.apache.thrift.TException;
import org.everthrift.appserver.model.DaoEntityIF;
import org.everthrift.thrift.TFunction;
import org.everthrift.thrift.TVoidFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;

import java.util.Map;

@SuppressWarnings("rawtypes")
public class CassandraFactories implements SmartLifecycle {

    public static class Result<E> {
        private final E r;

        private Result(E r) {
            this.r = r;
        }

        public E get() {
            return r;
        }

        public void no() {
        }
    }

    public static class VoidResult extends Result<Void> {
        private VoidResult() {
            super(null);
        }
    }

    private static final VoidResult NO_RESULT = new VoidResult();

    @Autowired
    private ApplicationContext ctx;

    @Autowired(required = false)
    private Session session;

    private Map<Class, CassandraModelFactory> factories = Maps.newHashMap();

    public synchronized void register(CassandraModelFactory f) {
        factories.put(f.getEntityClass(), f);
    }

    public synchronized <ENTITY extends DaoEntityIF> CassandraModelFactory<?, ENTITY, ?> of(Class<ENTITY> cls) {
        final CassandraModelFactory<?, ENTITY, ?> f = factories.get(cls);

        if (f == null) {
            throw new RuntimeException("Cound't find factory for " + cls.getCanonicalName());
        }

        return f;
    }

    public Statements begin() {
        return new Statements(this, session);
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public VoidResult batchApplyAndCommit(TVoidFunction<Statements> run) throws TException {
        Statements s = begin();
        run.apply(s);
        s.commit();
        return NO_RESULT;
    }

    public <E> Result<E> batchApplyAndCommit(TFunction<Statements, E> run) throws TException {
        Statements s = begin();
        final E ret = run.apply(s);
        s.commit();
        return new Result<E>(ret);
    }

    public VoidResult executeApplyAndCommit(TVoidFunction<Statements> run) throws TException {
        Statements s = begin().setBatch(false);
        run.apply(s);
        s.commit();
        return NO_RESULT;
    }

    public <E> Result<E> executeApplyAndCommit(TFunction<Statements, E> run) throws TException {
        Statements s = begin().setBatch(false);
        final E ret = run.apply(s);
        s.commit();
        return new Result<E>(ret);
    }

    @Override
    public void start() {
        for (CassandraModelFactory f : ctx.getBeansOfType(CassandraModelFactory.class).values()) {
            register(f);
        }
    }

    @Override
    public void stop() {

    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public int getPhase() {
        return 0;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable callback) {

    }

}

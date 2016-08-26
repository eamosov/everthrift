package org.everthrift.cassandra.model;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang.ArrayUtils;
import org.apache.thrift.TException;
import org.everthrift.appserver.model.CreatedAtIF;
import org.everthrift.appserver.model.DaoEntityIF;
import org.everthrift.appserver.model.UpdatedAtIF;
import org.everthrift.appserver.model.events.DeleteEntityEvent;
import org.everthrift.appserver.model.events.InsertEntityEvent;
import org.everthrift.appserver.model.events.UpdateEntityEvent;
import org.everthrift.cassandra.com.datastax.driver.mapping.Mapper.Option;
import org.everthrift.cassandra.com.datastax.driver.mapping.Mapper.UpdateQuery;
import org.everthrift.thrift.TFunction;
import org.everthrift.utils.LongTimestamp;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

public class Statements {

    private List<Statement> statements = Lists.newArrayList();

    @SuppressWarnings({"rawtypes"})
    private Multimap<CassandraModelFactory, Object> invalidates = Multimaps.newSetMultimap(Maps.newIdentityHashMap(),
                                                                                           () -> Sets.newHashSet());

    private List<Runnable> callbacks = Lists.newArrayList();

    private Session session;

    private boolean autoCommit = false;

    private boolean isBatch = true;

    private BatchStatement.Type batchType = BatchStatement.Type.LOGGED;

    private long timestamp; // in microseconds

    private CassandraFactories cassandraFactories;

    Statements(CassandraFactories cassandraFactories, Session session) {
        this.cassandraFactories = cassandraFactories;
        this.session = session;
        this.timestamp = LongTimestamp.nowMicros();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public <PK extends Serializable, ENTITY extends DaoEntityIF, E extends TException> CassandraModelFactory<PK, ENTITY, E> of(ENTITY e) {
        return (CassandraModelFactory) cassandraFactories.of(e.getClass());
    }

    @SuppressWarnings({"rawtypes"})
    private <ENTITY extends DaoEntityIF> void addStatement(final CassandraModelFactory f, final Statement s, ENTITY e) {
        if (s != null) {
            statements.add(s);
            invalidates.put(f, e.getPk());
        }
    }

    public <PK extends Serializable, ENTITY extends DaoEntityIF & UpdatedAtIF, E extends TException> void update(CassandraModelFactory<PK, ENTITY, E> factory,
                                                                                                                 PK pk,
                                                                                                                 Consumer<Assignments> assignment) {

        final Statement s = factory.mapper.updateQuery(assignment,
                                                       ArrayUtils.add(factory.extractCompaundPk(pk), Option.updatedAt(timestamp / 1000)));

        statements.add(s);
        invalidates.put(factory, pk);

        final ENTITY afterUpdate;
        try {
            afterUpdate = factory.getEntityClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }

        afterUpdate.setPk(pk);
        afterUpdate.setUpdatedAt(timestamp / 1000);

        final ENTITY beforeUpdate;
        try {
            beforeUpdate = factory.getEntityClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }

        beforeUpdate.setPk(pk);

        final UpdateEntityEvent<PK, ENTITY> event = factory.updateEntityEvent(beforeUpdate, afterUpdate);

        callbacks.add(() -> {
            factory.localEventBus.postEntityEvent(event);
        });

        if (autoCommit) {
            commit();
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public <ENTITY extends DaoEntityIF> ENTITY update(ENTITY e, TFunction<ENTITY, Boolean> mutator, Option... options) throws TException {
        final CassandraModelFactory factory = of(e);
        final ENTITY beforeUpdate = (ENTITY) factory.copy(e);

        final UpdateQuery uq = factory.updateQuery(beforeUpdate, e, mutator,
                                                   (Option[]) ArrayUtils.add(options, Option.updatedAt(timestamp / 1000)));
        if (uq == null) {
            return e;
        }

        addStatement(factory, uq.statement, e);

        final UpdateEntityEvent event = factory.updateEntityEvent(beforeUpdate, (ENTITY) factory.copy(e));
        callbacks.add(() -> {
            uq.applySpecialValues(e);
            factory.localEventBus.postEntityEvent(event);
        });

        if (autoCommit) {
            commit();
        }

        return e;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public <ENTITY extends DaoEntityIF> ENTITY save(ENTITY e, Option... options) {
        final CassandraModelFactory f = of(e);

        CreatedAtIF.setCreatedAt(e);
        addStatement(f, f.insertQuery(e, options), e);

        final ENTITY copy = (ENTITY) f.copy(e);
        final InsertEntityEvent event = f.insertEntityEvent(copy);
        callbacks.add(() -> {
            f.localEventBus.postEntityEvent(event);
        });

        if (autoCommit) {
            commit();
        }

        return e;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public <ENTITY extends DaoEntityIF> ENTITY delete(ENTITY e, Option... options) {
        final CassandraModelFactory f = of(e);

        final Serializable pk = e.getPk();
        statements.add(f.deleteQuery(pk, options));
        invalidates.put(f, pk);

        final DeleteEntityEvent event = f.deleteEntityEvent(e);
        callbacks.add(() -> {
            f.localEventBus.postEntityEvent(event);
        });

        if (autoCommit) {
            commit();
        }

        return e;
    }

    public ResultSet execute(Statement s) {
        return session.execute(s);
    }

    public ResultSetFuture executeAsync(Statement s) {
        return session.executeAsync(s);
    }

    public void add(Statement s) {
        statements.add(s);

        if (autoCommit) {
            commit();
        }
    }

    @SuppressWarnings({"rawtypes"})
    public <ENTITY extends DaoEntityIF> void invalidate(ENTITY e) {
        invalidate(of(e), e.getPk());
    }

    @SuppressWarnings({"rawtypes"})
    public void invalidate(CassandraModelFactory f, Object key) {
        invalidates.put(f, key);

        if (autoCommit) {
            commit();
        }
    }

    public void add(Runnable successCallback) {
        callbacks.add(() -> {
            successCallback.run();
        });

        if (autoCommit) {
            commit();
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public ListenableFuture<?> commitAsync(Executor executor) {
        final ListenableFuture f;

        if (isBatch() && statements.size() > 1) {
            final BatchStatement batch = new BatchStatement(batchType);
            batch.setDefaultTimestamp(timestamp);
            statements.forEach(s -> batch.add(s));
            f = executeAsync(batch);
        } else {
            final List<ResultSetFuture> ff = Lists.newArrayList();
            for (Statement s : statements) {
                ff.add(executeAsync(s));
            }
            f = Futures.successfulAsList(ff);
        }

        Futures.addCallback(f, new FutureCallback<Object>() {

            @Override
            public void onSuccess(Object result) {
                for (Map.Entry<CassandraModelFactory, Object> e : invalidates.entries()) {
                    e.getKey().invalidate(e.getValue());
                }

                callbacks.forEach(c -> c.run());
            }

            @Override
            public void onFailure(Throwable t) {
                t.printStackTrace();
            }
        });

        return f;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void commit() {

        if (isBatch() && statements.size() > 1) {
            final BatchStatement batch = new BatchStatement(batchType);
            batch.setIdempotent(false);
            batch.setDefaultTimestamp(timestamp);
            statements.forEach(s -> batch.add(s));
            execute(batch);
        } else {
            statements.forEach(s -> execute(s));
        }

        for (Map.Entry<CassandraModelFactory, Object> e : invalidates.entries()) {
            e.getKey().invalidate(e.getValue());
        }

        callbacks.forEach(c -> c.run());

        statements.clear();
        invalidates.clear();
        callbacks.clear();
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public Statements setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
        return this;
    }

    public boolean isBatch() {
        return isBatch;
    }

    public Statements setBatch(boolean isBatch) {
        this.isBatch = isBatch;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long currentTimeMillis() {
        return timestamp / 1000;
    }

    public BatchStatement.Type getBatchType() {
        return batchType;
    }

    public Statements setBatchType(BatchStatement.Type batchType) {
        this.batchType = batchType;
        return this;
    }
}

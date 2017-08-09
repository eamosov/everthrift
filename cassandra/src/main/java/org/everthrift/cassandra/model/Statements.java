package org.everthrift.cassandra.model;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import net.javacrumbs.futureconverter.java8guava.FutureConverter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.thrift.TException;
import org.everthrift.appserver.model.AbstractCachedModelFactory;
import org.everthrift.appserver.model.CreatedAtIF;
import org.everthrift.appserver.model.DaoEntityIF;
import org.everthrift.appserver.model.events.DeleteEntityEvent;
import org.everthrift.appserver.model.events.InsertEntityEvent;
import org.everthrift.appserver.model.events.UpdateEntityEvent;
import org.everthrift.cassandra.com.datastax.driver.mapping.Mapper.Option;
import org.everthrift.cassandra.com.datastax.driver.mapping.Mapper.UpdateQuery;
import org.everthrift.thrift.TFunction;
import org.everthrift.utils.ExceptionUtils;
import org.everthrift.utils.LongTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class Statements {

    private static final Logger log = LoggerFactory.getLogger(Statements.class);

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

    public <ENTITY extends DaoEntityIF> ENTITY updateUnchecked(ENTITY e, TFunction<ENTITY, Boolean> mutator, Option... options){
        return ExceptionUtils.asUnchecked(() -> update(e, mutator, options));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public <ENTITY extends DaoEntityIF> ENTITY update(ENTITY e, TFunction<ENTITY, Boolean> mutator, Option... options) throws TException {
        final CassandraModelFactory factory = of(e);
        final ENTITY beforeUpdate = (ENTITY) factory.copy(e);

        final UpdateQuery uq = factory.updateQuery(beforeUpdate, e, mutator,
                                                   (Option[]) ArrayUtils.add(options, Option.updatedAt(currentTimeMillis())));
        if (uq == null) {
            return e;
        }

        addStatement(factory, uq.statement, e);

        final UpdateEntityEvent event = factory.updateEntityEvent(beforeUpdate, (ENTITY) factory.copy(e));
        callbacks.add(() -> {
            uq.applySpecialValues(e);

            if (factory.localEventBus != null) {
                factory.localEventBus.postEntityEvent(event);
            }
        });

        if (autoCommit) {
            commit();
        }

        return e;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public <ENTITY extends DaoEntityIF> ENTITY save(ENTITY e, Option... options) {
        final CassandraModelFactory f = of(e);

        CreatedAtIF.setCreatedAt(e, currentTimeMillis());
        addStatement(f, f.insertQuery(e, options), e);

        final ENTITY copy = (ENTITY) f.copy(e);
        final InsertEntityEvent event = f.insertEntityEvent(copy);

        if (f.localEventBus != null) {
            callbacks.add(() -> {
                f.localEventBus.postEntityEvent(event);
            });
        }

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

        if (f.localEventBus != null) {
            callbacks.add(() -> {
                f.localEventBus.postEntityEvent(event);
            });
        }

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
    public CompletableFuture<?> commitAsync(Executor executor) {
        final CompletableFuture f;

        if (isBatch() && statements.size() > 1) {
            final BatchStatement batch = new BatchStatement(batchType);
            batch.setDefaultTimestamp(timestamp);
            statements.forEach(batch::add);
            f = FutureConverter.toCompletableFuture(executeAsync(batch));
        } else {
            final List<ResultSetFuture> ff = Lists.newArrayList();
            for (Statement s : statements) {
                ff.add(executeAsync(s));
            }
            f = FutureConverter.toCompletableFuture(Futures.allAsList(ff));
        }

        return f.whenComplete((result, t) -> {
            if (t != null) {
                log.error("Commit error", t);
            } else {
                for (Map.Entry<CassandraModelFactory, Object> e : invalidates.entries()) {
                    e.getKey().invalidate(e.getValue(), AbstractCachedModelFactory.InvalidateCause.UNKNOWN);
                }

                callbacks.forEach(Runnable::run);
            }
        });
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void commit() {

        if (isBatch() && statements.size() > 1) {
            final BatchStatement batch = new BatchStatement(batchType);
            batch.setIdempotent(false);
            batch.setDefaultTimestamp(timestamp);
            statements.forEach(batch::add);
            execute(batch);
        } else {
            statements.forEach(this::execute);
        }

        for (Map.Entry<CassandraModelFactory, Object> e : invalidates.entries()) {
            e.getKey().invalidate(e.getValue(), AbstractCachedModelFactory.InvalidateCause.UNKNOWN);
        }

        callbacks.forEach(Runnable::run);

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

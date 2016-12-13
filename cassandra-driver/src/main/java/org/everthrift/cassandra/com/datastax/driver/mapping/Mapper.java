/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.everthrift.cassandra.com.datastax.driver.mapping;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.everthrift.cassandra.com.datastax.driver.mapping.EntityMapper.Scenario;
import org.everthrift.cassandra.com.datastax.driver.mapping.Mapper.Option.SaveNullFields;
import org.everthrift.cassandra.com.datastax.driver.mapping.annotations.Accessor;
import org.everthrift.cassandra.com.datastax.driver.mapping.annotations.Computed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static org.everthrift.cassandra.com.datastax.driver.mapping.Mapper.Option.Type.SAVE_NULL_FIELDS;

/**
 * An object handling the mapping of a particular class.
 * <p/>
 * A {@code Mapper} object is obtained from a {@code MappingManager} using the
 * {@link MappingManager#mapper} method.
 */
public class Mapper<T> {

    private static final Logger logger = LoggerFactory.getLogger(EntityMapper.class);

    final MappingManager manager;

    final ProtocolVersion protocolVersion;

    final Class<T> klass;

    final EntityMapper<T> mapper;

    final TableMetadata tableMetadata;

    // Cache prepared statements for each type of query we use.
    private volatile Map<MapperQueryKey, PreparedStatement> preparedQueries = Collections.emptyMap();

    private static final Function<Object, Void> NOOP = Functions.constant(null);

    private volatile EnumMap<Option.Type, Option> defaultSaveOptions;

    private volatile EnumMap<Option.Type, Option> defaultUpdateOptions;

    private volatile EnumMap<Option.Type, Option> defaultGetOptions;

    private volatile EnumMap<Option.Type, Option> defaultGetAllOptions;

    private volatile EnumMap<Option.Type, Option> defaultDeleteOptions;

    private static final EnumMap<Option.Type, Option> NO_OPTIONS = new EnumMap<Option.Type, Option>(Option.Type.class);

    {
        NO_OPTIONS.put(Option.Type.SCENARIO, Option.scenario(Scenario.COMMON));
    }

    final Function<ResultSet, T> mapOneFunction;

    final Function<ResultSet, T> mapOneFunctionWithoutAliases;

    final Function<ResultSet, Result<T>> mapAllFunctionWithoutAliases;

    Mapper(MappingManager manager, Class<T> klass, EntityMapper<T> mapper) {
        this.manager = manager;
        this.klass = klass;
        this.mapper = mapper;

        KeyspaceMetadata keyspace = session().getCluster().getMetadata().getKeyspace(mapper.getKeyspace());
        this.tableMetadata = keyspace == null ? null : keyspace.getTable(mapper.getTable());

        this.protocolVersion = manager.getSession()
                                      .getCluster()
                                      .getConfiguration()
                                      .getProtocolOptions()
                                      .getProtocolVersion();
        this.mapOneFunction = new Function<ResultSet, T>() {
            @Override
            public T apply(ResultSet rs) {
                return Mapper.this.mapAliased(rs).one();
            }
        };
        this.mapOneFunctionWithoutAliases = new Function<ResultSet, T>() {
            @Override
            public T apply(ResultSet rs) {
                return Mapper.this.map(rs).one();
            }
        };
        this.mapAllFunctionWithoutAliases = new Function<ResultSet, Result<T>>() {
            @Override
            public Result<T> apply(ResultSet rs) {
                return Mapper.this.map(rs);
            }
        };

        this.defaultSaveOptions = NO_OPTIONS;
        this.defaultUpdateOptions = NO_OPTIONS;
        this.defaultGetOptions = NO_OPTIONS;
        this.defaultGetAllOptions = NO_OPTIONS;
        this.defaultDeleteOptions = NO_OPTIONS;
    }

    Session session() {
        return manager.getSession();
    }

    public int primaryKeySize() {
        return mapper.primaryKeySize();
    }

    public ColumnMapper<T> getPrimaryKeyColumn(int i) {
        return mapper.getPrimaryKeyColumn(i);
    }

    public ColumnMapper<T> getColumnByFieldName(final String fieldName) {
        return mapper.getColumnByFieldName(fieldName);
    }

    public Set<ColumnMapper<T>> allColumns(Scenario scenario) {
        return mapper.allColumns(scenario);
    }

    public String getTableName() {
        return mapper.getTable();
    }

    public ColumnMapper<T> getVersionColumn() {
        return mapper.getVersionColumn();
    }

    public ColumnMapper<T> getUpdatedAtColumn() {
        return mapper.getUpdatedAtColumn();
    }

    PreparedStatement getPreparedQuery(QueryType type, Set<ColumnMapper<?>> columns, EnumMap<Option.Type, Option> options) {

        MapperQueryKey pqk = new MapperQueryKey(type, columns, options);

        PreparedStatement stmt = preparedQueries.get(pqk);
        if (stmt == null) {
            synchronized (preparedQueries) {
                stmt = preparedQueries.get(pqk);
                if (stmt == null) {
                    String queryString = type.makePreparedQueryString(tableMetadata, mapper, manager, columns, options);
                    logger.debug("Preparing query {}", queryString);
                    stmt = session().prepare(queryString);
                    Map<MapperQueryKey, PreparedStatement> newQueries = new HashMap<MapperQueryKey, PreparedStatement>(preparedQueries);
                    newQueries.put(pqk, stmt);
                    preparedQueries = newQueries;
                }
            }
        }
        return stmt;
    }

    /**
     * The {@code TableMetadata} for this mapper.
     *
     * @return the {@code TableMetadata} for this mapper or {@code null} if
     * keyspace is not set.
     */
    public TableMetadata getTableMetadata() {
        return tableMetadata;
    }

    /**
     * The {@code MappingManager} managing this mapper.
     *
     * @return the {@code MappingManager} managing this mapper.
     */
    public MappingManager getManager() {
        return manager;
    }

    /**
     * Creates a query that can be used to save the provided entity.
     * <p/>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it
     * manually or need access to the {@code ResultSet} object after execution
     * (to get the trace, the execution info, ...), but in other cases, calling
     * {@link #save} or {@link #saveAsync} is shorter.
     *
     * @param entity the entity to save.
     * @return a query that saves {@code entity} (based on it's defined
     * mapping).
     */
    public Statement saveQuery(T entity) {
        return saveQuery(entity, this.defaultSaveOptions);
    }

    /**
     * Creates a query that can be used to save the provided entity.
     * <p/>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it
     * manually or need access to the {@code ResultSet} object after execution
     * (to get the trace, the execution info, ...), but in other cases, calling
     * {@link #save} or {@link #saveAsync} is shorter. This method allows you to
     * provide a suite of {@link Option} to include in the SAVE query. Options
     * currently supported for SAVE are :
     * <ul>
     * <li>Timestamp</li>
     * <li>Time-to-live (ttl)</li>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param entity the entity to save.
     * @return a query that saves {@code entity} (based on it's defined
     * mapping).
     */
    public Statement saveQuery(T entity, Option... options) {
        return saveQuery(entity, toMapWithDefaults(options, this.defaultSaveOptions));
    }

    private Statement saveQuery(T entity, EnumMap<Option.Type, Option> options) {
        Map<ColumnMapper<?>, Object> values = new HashMap<ColumnMapper<?>, Object>();
        boolean saveNullFields = shouldSaveNullFields(options);

        for (ColumnMapper<T> cm : mapper.allColumns(((Option.Scenario) options.get(Option.Type.SCENARIO)).getScenario())) {
            Object value = cm.getValue(entity);
            if (cm.kind != ColumnMapper.Kind.COMPUTED && (saveNullFields || value != null)) {
                values.put(cm, value);
            }
        }

        BoundStatement bs = getPreparedQuery(QueryType.SAVE, values.keySet(), options).bind();
        int i = 0;
        for (Map.Entry<ColumnMapper<?>, Object> entry : values.entrySet()) {
            ColumnMapper<?> mapper = entry.getKey();
            Object value = entry.getValue();
            setObject(bs, i++, value, mapper);
        }

        if (mapper.writeConsistency != null) {
            bs.setConsistencyLevel(mapper.writeConsistency);
        }

        for (Option opt : options.values()) {
            opt.checkValidFor(QueryType.SAVE, manager);
            opt.addToPreparedStatement(bs, i++);
        }

        return bs;
    }

    public UpdateQuery updateQuery(T entity, Option... options) throws NotModifiedException {
        return updateQuery(null, entity, toMapWithDefaults(options, this.defaultUpdateOptions));
    }

    public UpdateQuery updateQuery(T beforeUpdate, T afterUpdate, Option... options) throws NotModifiedException {
        return updateQuery(beforeUpdate, afterUpdate, toMapWithDefaults(options, this.defaultUpdateOptions));
    }

    private Number inc(Object value) {
        if (value instanceof Integer) {
            return (Integer) value + 1;
        } else if (value instanceof Long) {
            return (Long) value + 1;
        } else if (value == null) {
            return 1;
        } else {
            throw new RuntimeException("invalid type for version column: " + value.getClass().getCanonicalName());
        }
    }

    public static class UpdateQuery {
        public final Statement statement;

        public final Map<ColumnMapper, Object> specialValues; // version and
        // updatedAt

        UpdateQuery(Statement statement, Map<ColumnMapper, Object> specialValues) {
            super();
            this.statement = statement;
            this.specialValues = specialValues;
        }

        public void applySpecialValues(Object entity) {
            for (Entry<ColumnMapper, Object> e : specialValues.entrySet()) {
                e.getKey().setValue(entity, e.getValue());
            }
        }
    }

    private UpdateQuery updateQuery(T beforeUpdate, T afterUpdate, EnumMap<Option.Type, Option> options) throws NotModifiedException {

        if (afterUpdate == beforeUpdate) {
            throw new IllegalArgumentException();
        }

        boolean saveNullFields = shouldSaveNullFields(options);

        boolean isOptimisticUpdate = options.containsKey(Option.Type.ONLY_IF);

        if (isOptimisticUpdate && mapper.getVersionColumn() == null) {
            throw new RuntimeException("ONLY_IF may be used only with versioned entities");
        }

        final Map<ColumnMapper<T>, Object> values = new HashMap<ColumnMapper<T>, Object>();
        final Map<ColumnMapper<T>, Object> specialValues = new HashMap<ColumnMapper<T>, Object>();

        int nUpdatedFields = 0;

        final Option.UpdatedAt updatedAtOption = (Option.UpdatedAt) options.get(Option.Type.UPDATED_AT);

        if (updatedAtOption != null && mapper.getUpdatedAtColumn() == null) {
            throw new RuntimeException("Option.UpdatedAt needs updatedAtColumn");
        }

        Object versionBefore = null;

        for (ColumnMapper<T> cm : mapper.allColumns(((Option.Scenario) options.get(Option.Type.SCENARIO)).getScenario())) {
            final Object value = cm.getValue(afterUpdate);

            if (beforeUpdate == null) {
                if (cm.kind == ColumnMapper.Kind.REGULAR && (saveNullFields || value != null)) {
                    values.put(cm, value);
                    nUpdatedFields++;
                }
            } else {
                final Object origValue = cm.getValue(beforeUpdate);

                if (mapper.isVersion(cm) && isOptimisticUpdate) {

                    if (!Objects.equal(value, origValue)) {
                        throw new IllegalArgumentException("entity and original must have the same version");
                    }

                    versionBefore = origValue;

                    final Object _value;
                    if (mapper.isUpdatedAt(cm)) {
                        _value = updatedAtOption != null ? updatedAtOption.getValue() : System.currentTimeMillis();
                    } else {
                        _value = inc(value);
                    }

                    values.put(cm, _value);
                    specialValues.put(cm, _value);
                } else if (mapper.isUpdatedAt(cm)) {

                    final Object _value;

                    if (!Objects.equal(value, origValue)) {
                        nUpdatedFields++;
                        _value = value;
                    }else{
                        _value = updatedAtOption != null ? updatedAtOption.getValue() : System.currentTimeMillis();
                    }

                    values.put(cm, _value);
                    specialValues.put(cm, _value);

                } else if (cm.kind == ColumnMapper.Kind.REGULAR) {
                    if (!Objects.equal(value, origValue)) {
                        values.put(cm, value);
                        nUpdatedFields++;
                    }
                } else if (!Objects.equal(value, origValue)) {
                    throw new IllegalArgumentException("entity must have the same non regular fields");
                }
            }
        }

        if (nUpdatedFields == 0) {
            throw new NotModifiedException();
        }

        final BoundStatement bs = getPreparedQuery(QueryType.UPDATE, (Set) values.keySet(), options).bind();
        int i = 0;
        for (Map.Entry<ColumnMapper<T>, Object> entry : values.entrySet()) {
            setObject(bs, i++, entry.getValue(), entry.getKey());
        }

        for (int j = 0; j < mapper.primaryKeySize(); j++) {
            ColumnMapper<T> cm = mapper.getPrimaryKeyColumn(j);
            Object value = cm.getValue(afterUpdate);
            setObject(bs, i++, value, cm);
        }

        if (isOptimisticUpdate) {
            setObject(bs, i++, versionBefore, mapper.getVersionColumn());
        }

        if (mapper.writeConsistency != null) {
            bs.setConsistencyLevel(mapper.writeConsistency);
        }

        for (Option opt : options.values()) {
            opt.checkValidFor(QueryType.UPDATE, manager);
            opt.addToPreparedStatement(bs, i++);
        }

        return new UpdateQuery(bs, (Map) specialValues);
    }

    public Statement updateQuery(Consumer<Assignments> assignment, Object... objects) {
        List<Object> pks = new ArrayList<Object>();
        EnumMap<Option.Type, Option> options = new EnumMap<Option.Type, Option>(defaultUpdateOptions);

        for (Object o : objects) {
            if (o instanceof Option) {
                Option option = (Option) o;
                options.put(option.type, option);
            } else {
                pks.add(o);
            }
        }
        return updateQuery(assignment, pks, options);
    }

    private Statement updateQuery(Consumer<Assignments> assignment, List<Object> pks, EnumMap<Option.Type, Option> options) {

        final Option.UpdatedAt updatedAtOption = (Option.UpdatedAt) options.get(Option.Type.UPDATED_AT);

        if (updatedAtOption != null && mapper.getUpdatedAtColumn() == null) {
            throw new RuntimeException("Option.UpdatedAt needs updatedAtColumn");
        }

        final Update update = QueryBuilder.update(mapper.getTable());
        final Assignments assignments = update.with();

        if (mapper.getUpdatedAtColumn() != null) {
            assignments.and(QueryBuilder.set(mapper.getUpdatedAtColumn().getColumnName(),
                                             new Date(updatedAtOption != null ? updatedAtOption.getValue() : System.currentTimeMillis())));
        }

        assignment.accept(assignments);

        final Update.Where uWhere = update.where();
        for (int i = 0; i < mapper.primaryKeySize(); i++) {
            uWhere.and(QueryBuilder.eq(mapper.getPrimaryKeyColumn(i).getColumnNameUnquoted(), QueryBuilder.bindMarker()));
        }

        update.setConsistencyLevel(getWriteConsistency());

        return session().prepare(update).bind(pks.toArray(new Object[pks.size()]));
    }

    private static boolean shouldSaveNullFields(EnumMap<Option.Type, Option> options) {
        SaveNullFields option = (SaveNullFields) options.get(SAVE_NULL_FIELDS);
        return option == null || option.saveNullFields;
    }

    private void setObject(BoundStatement bs, int i, Object value, ColumnMapper<?> mapper) {
        try {
            TypeCodec<Object> customCodec = mapper.getCustomCodec();
            if (customCodec != null) {
                bs.set(i, value, customCodec);
            } else {
                bs.set(i, value, mapper.getJavaType());
            }
        } catch (Exception e) {
            logger.error(String.format("Error setting %s.%s, query='%s', i=%d, value=%s", getTableName(), mapper.getColumnNameUnquoted(),
                                       bs.preparedStatement().getQueryString(), i, value),
                         e);
            throw Throwables.propagate(e);
        }
    }

    private boolean isApplied(ResultSet rs) {
        final List<Row> all = rs.all();
        return all.isEmpty() || all.get(0).getBool(0);
    }

    /**
     * Save an entity mapped by this mapper.
     * <p/>
     * This method is basically equivalent to:
     * {@code getManager().getSession().execute(saveQuery(entity))}.
     *
     * @param entity the entity to save.
     */
    public boolean save(T entity) {
        return isApplied(session().execute(saveQuery(entity)));
    }

    /**
     * Save an entity mapped by this mapper and using special options for save.
     * This method allows you to provide a suite of {@link Option} to include in
     * the SAVE query. Options currently supported for SAVE are :
     * <ul>
     * <li>Timestamp</li>
     * <li>Time-to-live (ttl)</li>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param entity  the entity to save.
     * @param options the options object specified defining special options when
     *                saving.
     */
    public boolean save(T entity, Option... options) {
        return isApplied(session().execute(saveQuery(entity, options)));
    }

    /**
     * Save an entity mapped by this mapper asynchronously.
     * <p/>
     * This method is basically equivalent to:
     * {@code getManager().getSession().executeAsync(saveQuery(entity))}.
     *
     * @param entity the entity to save.
     * @return a future on the completion of the save operation.
     */
    public ListenableFuture<Boolean> saveAsync(T entity) {
        return Futures.transform(session().executeAsync(saveQuery(entity)), new Function<ResultSet, Boolean>() {

            @Override
            public Boolean apply(ResultSet input) {
                return isApplied(input);
            }
        });
    }

    /**
     * Save an entity mapped by this mapper asynchronously using special options
     * for save.
     * <p/>
     * This method is basically equivalent to:
     * {@code getManager().getSession().executeAsync(saveQuery(entity, options))}.
     *
     * @param entity  the entity to save.
     * @param options the options object specified defining special options when
     *                saving.
     * @return a future on the completion of the save operation.
     */
    public ListenableFuture<Boolean> saveAsync(T entity, Option... options) {
        return Futures.transform(session().executeAsync(saveQuery(entity, options)), new Function<ResultSet, Boolean>() {

            @Override
            public Boolean apply(ResultSet input) {
                return isApplied(input);
            }
        });
    }

    public boolean update(T entity) throws VersionException {
        return update(null, entity);
    }

    public boolean update(T beforeUpdate, T afterUpdate, Option... options) throws VersionException {
        final ResultSet rs;
        final UpdateQuery uq;
        try {
            uq = updateQuery(beforeUpdate, afterUpdate, options);
        } catch (NotModifiedException e) {
            logger.debug("NotModifiedException");
            return false;
        }

        rs = session().execute(uq.statement);

        final List<Row> all = rs.all();
        if (all.isEmpty()) { // no optimistic locking
            uq.applySpecialValues(afterUpdate);
            return true;
        }

        final boolean applied = all.get(0).getBool(0);
        if (applied) {
            uq.applySpecialValues(afterUpdate);
        } else if (mapper.getVersionColumn() != null && rs.getColumnDefinitions().contains(mapper.getVersionColumn()
                                                                                                 .getColumnName())) {
            throw new VersionException(all.get(0).getObject(mapper.getVersionColumn().getColumnName()));
        }

        return applied;
    }

    /**
     * Creates a query to fetch entity given its PRIMARY KEY.
     * <p/>
     * The values provided must correspond to the columns composing the PRIMARY
     * KEY (in the order of said primary key).
     * <p/>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it
     * manually, but in other cases, calling {@link #get} or {@link #getAsync}
     * is shorter.
     * <p/>
     * This method allows you to provide a suite of {@link Option} to include in
     * the GET query. Options currently supported for GET are :
     * <ul>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param objects the primary key of the entity to fetch, or more precisely
     *                the values for the columns of said primary key in the order of the
     *                primary key. Can be followed by {@link Option} to include in the DELETE
     *                query.
     * @return a query that fetch the entity of PRIMARY KEY {@code objects}.
     * @throws IllegalArgumentException if the number of value provided differ
     *                                  from the number of columns composing the PRIMARY KEY of the mapped class,
     *                                  or if at least one of those values is {@code null}.
     */
    public Statement getQuery(Object... objects) {
        // Order and duplicates matter for primary keys
        List<Object> pks = new ArrayList<Object>();
        EnumMap<Option.Type, Option> options = new EnumMap<Option.Type, Option>(defaultGetOptions);

        for (Object o : objects) {
            if (o instanceof Option) {
                Option option = (Option) o;
                options.put(option.type, option);
            } else {
                pks.add(o);
            }
        }
        return getQuery(pks, options);
    }

    private Statement getQuery(List<Object> primaryKeys, EnumMap<Option.Type, Option> options) {

        if (primaryKeys.size() != mapper.primaryKeySize()) {
            throw new IllegalArgumentException(String.format("Invalid number of PRIMARY KEY columns provided, %d expected but got %d",
                                                             mapper.primaryKeySize(), primaryKeys.size()));
        }

        BoundStatement bs = getPreparedQuery(QueryType.GET,
                                             (Set) mapper.allColumns(((Option.Scenario) options.get(Option.Type.SCENARIO))
                                                                         .getScenario()),
                                             options).bind();
        int i = 0;
        for (Object value : primaryKeys) {
            ColumnMapper<T> column = mapper.getPrimaryKeyColumn(i);
            if (value == null) {
                throw new IllegalArgumentException(String.format("Invalid null value for PRIMARY KEY column %s (argument %d)",
                                                                 column.getColumnName(), i));
            }
            setObject(bs, i++, value, column);
        }

        if (mapper.readConsistency != null) {
            bs.setConsistencyLevel(mapper.readConsistency);
        }

        for (Option opt : options.values()) {
            opt.checkValidFor(QueryType.GET, manager);
            opt.addToPreparedStatement(bs, i);
            if (opt.isIncludedInQuery()) {
                i++;
            }
        }

        return bs;
    }

    private Statement getAllQuery(EnumMap<Option.Type, Option> options) {

        BoundStatement bs = getPreparedQuery(QueryType.GET_ALL,
                                             (Set) mapper.allColumns(((Option.Scenario) options.get(Option.Type.SCENARIO))
                                                                         .getScenario()),
                                             options).bind();
        int i = 0;

        if (mapper.readConsistency != null) {
            bs.setConsistencyLevel(mapper.readConsistency);
        }

        for (Option opt : options.values()) {
            opt.checkValidFor(QueryType.GET_ALL, manager);
            opt.addToPreparedStatement(bs, i);
            if (opt.isIncludedInQuery()) {
                i++;
            }
        }
        return bs;
    }

    public Statement getAllQuery(Option... _options) {
        EnumMap<Option.Type, Option> options = new EnumMap<Option.Type, Option>(defaultGetAllOptions);

        for (Option o : _options) {
            options.put(o.type, o);
        }
        return getAllQuery(options);
    }

    /**
     * Fetch an entity based on its primary key.
     * <p/>
     * This method is basically equivalent to:
     * {@code map(getManager().getSession().execute(getQuery(objects))).one()}.
     *
     * @param objects the primary key of the entity to fetch, or more precisely
     *                the values for the columns of said primary key in the order of the
     *                primary key. Can be followed by {@link Option} to include in the DELETE
     *                query.
     * @return the entity fetched or {@code null} if it doesn't exist.
     * @throws IllegalArgumentException if the number of value provided differ
     *                                  from the number of columns composing the PRIMARY KEY of the mapped class,
     *                                  or if at least one of those values is {@code null}.
     */
    public T get(Object... objects) {
        return mapAliased(session().execute(getQuery(objects))).one();
    }

    /**
     * Fetch an entity based on its primary key asynchronously.
     * <p/>
     * This method is basically equivalent to mapping the result of:
     * {@code getManager().getSession().executeAsync(getQuery(objects))}.
     *
     * @param objects the primary key of the entity to fetch, or more precisely
     *                the values for the columns of said primary key in the order of the
     *                primary key. Can be followed by {@link Option} to include in the DELETE
     *                query.
     * @return a future on the fetched entity. The return future will yield
     * {@code null} if said entity doesn't exist.
     * @throws IllegalArgumentException if the number of value provided differ
     *                                  from the number of columns composing the PRIMARY KEY of the mapped class,
     *                                  or if at least one of those values is {@code null}.
     */
    public ListenableFuture<T> getAsync(Object... objects) {
        return Futures.transform(session().executeAsync(getQuery(objects)), mapOneFunction);
    }

    public Result<T> getAll(Option... options) {
        return mapAliased(session().execute(getAllQuery(options)));
    }

    /**
     * Creates a query that can be used to delete the provided entity.
     * <p/>
     * This method is a shortcut that extract the PRIMARY KEY from the provided
     * entity and call {@link #deleteQuery(Object...)} with it. This method
     * allows you to provide a suite of {@link Option} to include in the DELETE
     * query. Note : currently, only
     * {@link org.everthrift.cassandra.com.datastax.driver.mapping.Mapper.Option.Timestamp}
     * is supported for DELETE queries.
     * <p/>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it
     * manually or need access to the {@code ResultSet} object after execution
     * (to get the trace, the execution info, ...), but in other cases, calling
     * {@link #delete} or {@link #deleteAsync} is shorter.
     * <p/>
     * This method allows you to provide a suite of {@link Option} to include in
     * the DELETE query. Options currently supported for DELETE are :
     * <ul>
     * <li>Timestamp</li>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param entity  the entity to delete.
     * @param options the options to add to the DELETE query.
     * @return a query that delete {@code entity} (based on it's defined
     * mapping) with provided USING options.
     */
    public Statement deleteQuery(T entity, Option... options) {
        List<Object> pks = new ArrayList<Object>();
        for (int i = 0; i < mapper.primaryKeySize(); i++) {
            pks.add(mapper.getPrimaryKeyColumn(i).getValue(entity));
        }

        return deleteQuery(pks, toMapWithDefaults(options, defaultDeleteOptions));
    }

    /**
     * Creates a query that can be used to delete the provided entity.
     * <p/>
     * This method is a shortcut that extract the PRIMARY KEY from the provided
     * entity and call {@link #deleteQuery(Object...)} with it.
     * <p/>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it
     * manually or need access to the {@code ResultSet} object after execution
     * (to get the trace, the execution info, ...), but in other cases, calling
     * {@link #delete} or {@link #deleteAsync} is shorter.
     *
     * @param entity the entity to delete.
     * @return a query that delete {@code entity} (based on it's defined
     * mapping).
     */
    public Statement deleteQuery(T entity) {
        List<Object> pks = new ArrayList<Object>();
        for (int i = 0; i < mapper.primaryKeySize(); i++) {
            pks.add(mapper.getPrimaryKeyColumn(i).getValue(entity));
        }

        return deleteQuery(pks, defaultDeleteOptions);
    }

    /**
     * Creates a query that can be used to delete an entity given its PRIMARY
     * KEY.
     * <p/>
     * The values provided must correspond to the columns composing the PRIMARY
     * KEY (in the order of said primary key). The values can also contain,
     * after specifying the primary keys columns, a suite of {@link Option} to
     * include in the DELETE query. Note : currently, only
     * {@link org.everthrift.cassandra.com.datastax.driver.mapping.Mapper.Option.Timestamp}
     * is supported for DELETE queries.
     * <p/>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it
     * manually or need access to the {@code ResultSet} object after execution
     * (to get the trace, the execution info, ...), but in other cases, calling
     * {@link #delete} or {@link #deleteAsync} is shorter. This method allows
     * you to provide a suite of {@link Option} to include in the DELETE query.
     * Options currently supported for DELETE are :
     * <ul>
     * <li>Timestamp</li>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param objects the primary key of the entity to delete, or more precisely
     *                the values for the columns of said primary key in the order of the
     *                primary key. Can be followed by {@link Option} to include in the DELETE
     *                query.
     * @return a query that delete the entity of PRIMARY KEY {@code primaryKey}.
     * @throws IllegalArgumentException if the number of value provided differ
     *                                  from the number of columns composing the PRIMARY KEY of the mapped class,
     *                                  or if at least one of those values is {@code null}.
     */
    public Statement deleteQuery(Object... objects) {
        // Order and duplicates matter for primary keys
        List<Object> pks = new ArrayList<Object>();
        EnumMap<Option.Type, Option> options = new EnumMap<Option.Type, Option>(defaultDeleteOptions);

        for (Object o : objects) {
            if (o instanceof Option) {
                Option option = (Option) o;
                options.put(option.type, option);
            } else {
                pks.add(o);
            }
        }
        return deleteQuery(pks, options);
    }

    private Statement deleteQuery(List<Object> primaryKey, EnumMap<Option.Type, Option> options) {
        if (primaryKey.size() != mapper.primaryKeySize()) {
            throw new IllegalArgumentException(String.format("Invalid number of PRIMARY KEY columns provided, %d expected but got %d",
                                                             mapper.primaryKeySize(), primaryKey.size()));
        }

        BoundStatement bs = getPreparedQuery(QueryType.DEL, Collections.emptySet(), options).bind();

        if (mapper.writeConsistency != null) {
            bs.setConsistencyLevel(mapper.writeConsistency);
        }

        int i = 0;
        for (Option opt : options.values()) {
            opt.checkValidFor(QueryType.DEL, manager);
            opt.addToPreparedStatement(bs, i);
            if (opt.isIncludedInQuery()) {
                i++;
            }
        }

        int columnNumber = 0;
        for (Object value : primaryKey) {
            ColumnMapper<T> column = mapper.getPrimaryKeyColumn(columnNumber);
            if (value == null) {
                throw new IllegalArgumentException(String.format("Invalid null value for PRIMARY KEY column %s (argument %d)",
                                                                 column.getColumnName(), i));
            }
            setObject(bs, i++, value, column);
            columnNumber++;
        }
        return bs;
    }

    /**
     * Deletes an entity mapped by this mapper.
     * <p/>
     * This method is basically equivalent to:
     * {@code getManager().getSession().execute(deleteQuery(entity))}.
     *
     * @param entity the entity to delete.
     */
    public void delete(T entity) {
        session().execute(deleteQuery(entity));
    }

    /**
     * Deletes an entity mapped by this mapper using provided options.
     * <p/>
     * This method is basically equivalent to:
     * {@code getManager().getSession().execute(deleteQuery(entity, options))}.
     *
     * @param entity  the entity to delete.
     * @param options the options to add to the DELETE query.
     */
    public void delete(T entity, Option... options) {
        session().execute(deleteQuery(entity, options));
    }

    /**
     * Deletes an entity mapped by this mapper asynchronously.
     * <p/>
     * This method is basically equivalent to:
     * {@code getManager().getSession().executeAsync(deleteQuery(entity))}.
     *
     * @param entity the entity to delete.
     * @return a future on the completion of the deletion.
     */
    public ListenableFuture<Void> deleteAsync(T entity) {
        return Futures.transform(session().executeAsync(deleteQuery(entity)), NOOP);
    }

    /**
     * Deletes an entity mapped by this mapper asynchronously using provided
     * options.
     * <p/>
     * This method is basically equivalent to:
     * {@code getManager().getSession().executeAsync(deleteQuery(entity, options))}.
     *
     * @param entity  the entity to delete.
     * @param options the options to add to the DELETE query.
     * @return a future on the completion of the deletion.
     */
    public ListenableFuture<Void> deleteAsync(T entity, Option... options) {
        return Futures.transform(session().executeAsync(deleteQuery(entity, options)), NOOP);
    }

    /**
     * Deletes an entity based on its primary key.
     * <p/>
     * This method is basically equivalent to:
     * {@code getManager().getSession().execute(deleteQuery(objects))}.
     *
     * @param objects the primary key of the entity to delete, or more precisely
     *                the values for the columns of said primary key in the order of the
     *                primary key.Can be followed by {@link Option} to include in the DELETE
     *                query.
     * @throws IllegalArgumentException if the number of value provided differ
     *                                  from the number of columns composing the PRIMARY KEY of the mapped class,
     *                                  or if at least one of those values is {@code null}.
     */
    public void delete(Object... objects) {
        session().execute(deleteQuery(objects));
    }

    /**
     * Deletes an entity based on its primary key asynchronously.
     * <p/>
     * This method is basically equivalent to:
     * {@code getManager().getSession().executeAsync(deleteQuery(objects))}.
     *
     * @param objects the primary key of the entity to delete, or more precisely
     *                the values for the columns of said primary key in the order of the
     *                primary key. Can be followed by {@link Option} to include in the DELETE
     *                query.
     * @throws IllegalArgumentException if the number of value provided differ
     *                                  from the number of columns composing the PRIMARY KEY of the mapped class,
     *                                  or if at least one of those values is {@code null}.
     */
    public ListenableFuture<Void> deleteAsync(Object... objects) {
        return Futures.transform(session().executeAsync(deleteQuery(objects)), NOOP);
    }

    /**
     * Maps the rows from a {@code ResultSet} into the class this is a mapper
     * of.
     * <p/>
     * Use this method to map a result set that was not generated by the mapper
     * (e.g. a result set coming from a manual query or an {@link Accessor
     * method}. It expects that the result set contains all column mapped in the
     * target class, and that they are not aliased. {@link Computed} fields will
     * not be filled in mapped objects.
     *
     * @param resultSet the {@code ResultSet} to map.
     * @return the mapped result set. Note that the returned mapped result set
     * will encapsulate {@code resultSet} and so consuming results from this
     * returned mapped result set will consume results from {@code resultSet}
     * and vice-versa.
     * @see #mapAliased(ResultSet)
     */
    public Result<T> map(ResultSet resultSet) {
        return new Result<T>(resultSet, mapper, protocolVersion);
    }

    /**
     * Maps the rows from a {@code ResultSet} into the class this is a mapper
     * of.
     * <p/>
     * Use this method to map a result set coming from the execution of the
     * {@code Statement} returned by {@link #getQuery(Object...)}.
     * {@link Computed} fields will be filled in the mapped object.
     *
     * @param resultSet the {@code ResultSet} to map.
     * @return the mapped result set. Note that the returned mapped result set
     * will encapsulate {@code resultSet} and so consuming results from this
     * returned mapped result set will consume results from {@code resultSet}
     * and vice-versa.
     * @see #map(ResultSet)
     */
    public Result<T> mapAliased(ResultSet resultSet) {
        return (manager.isCassandraV1) ? map(resultSet) // no aliases
                                       : new Result<T>(resultSet, mapper, protocolVersion, true);
    }

    /**
     * Set the default save {@link Option} for this object mapper, that will be
     * used in all save operations unless overridden. Refer to
     * {@link Mapper#save(Object, Option...)})} to check available save options.
     *
     * @param options the options to set. To reset, use
     *                {@link Mapper#resetDefaultSaveOptions}.
     */
    public void setDefaultSaveOptions(Option... options) {
        this.defaultSaveOptions = toMap(options);
    }

    /**
     * Reset the default save options for this object mapper.
     */
    public void resetDefaultSaveOptions() {
        this.defaultSaveOptions = NO_OPTIONS;
    }

    public void setDefaultUpdateOptions(Option... options) {
        this.defaultSaveOptions = toMap(options);
    }

    public void resetDefaultUpdateOptions() {
        this.defaultUpdateOptions = NO_OPTIONS;
    }

    /**
     * Set the default get {@link Option} for this object mapper, that will be
     * used in all get operations unless overridden. Refer to
     * {@link Mapper#get(Object...)} )} to check available get options.
     *
     * @param options the options to set. To reset, use
     *                {@link Mapper#resetDefaultGetOptions}.
     */
    public void setDefaultGetOptions(Option... options) {
        this.defaultGetOptions = toMap(options);

    }

    /**
     * Reset the default save options for this object mapper.
     */
    public void resetDefaultGetOptions() {
        this.defaultGetOptions = NO_OPTIONS;
    }

    public void setDefaultGetAllOptions(Option... options) {
        this.defaultGetAllOptions = toMap(options);

    }

    public void resetDefaultGetAllOptions() {
        this.defaultGetAllOptions = NO_OPTIONS;
    }

    /**
     * Set the default delete {@link Option} for this object mapper, that will
     * be used in all delete operations unless overridden. Refer to
     * {@link Mapper#delete(Object...)} )} to check available delete options.
     *
     * @param options the options to set. To reset, use
     *                {@link Mapper#resetDefaultDeleteOptions}.
     */
    public void setDefaultDeleteOptions(Option... options) {
        this.defaultDeleteOptions = toMap(options);
    }

    /**
     * Reset the default delete options for this object mapper.
     */
    public void resetDefaultDeleteOptions() {
        this.defaultDeleteOptions = NO_OPTIONS;
    }

    private static EnumMap<Option.Type, Option> toMap(Option[] options) {
        EnumMap<Option.Type, Option> result = new EnumMap<Option.Type, Option>(Option.Type.class);
        for (Option option : options) {
            result.put(option.type, option);
        }
        return result;
    }

    private static EnumMap<Option.Type, Option> toMapWithDefaults(Option[] options, EnumMap<Option.Type, Option> defaults) {
        EnumMap<Option.Type, Option> result = new EnumMap<Option.Type, Option>(defaults);
        for (Option option : options) {
            result.put(option.type, option);
        }
        return result;
    }

    public ConsistencyLevel getReadConsistency() {
        return mapper.readConsistency;
    }

    public void setReadConsistency(ConsistencyLevel level) {
        mapper.readConsistency = level;
    }

    public ConsistencyLevel getWriteConsistency() {
        return mapper.writeConsistency;
    }

    public void setWriteConsistency(ConsistencyLevel level) {
        mapper.writeConsistency = level;
    }

    /**
     * An option for a mapper operation.
     * <p/>
     * Options can be passed to individual operations:
     * <p>
     * <pre>
     * mapper.save(myObject, Option.ttl(3600));
     * </pre>
     * <p/>
     * The mapper can also have defaults, that will apply to all operations that
     * do not override these particular option:
     * <p>
     * <pre>
     * mapper.setDefaultSaveOptions(Option.ttl(3600));
     * mapper.save(myObject);
     * </pre>
     * <p/>
     * <p/>
     * See the static methods in this class for available options.
     */
    public static abstract class Option {

        enum Type {
            TTL,
            TIMESTAMP,
            CL,
            TRACING,
            SAVE_NULL_FIELDS,
            IF_NOT_EXISTS,
            IF_EXISTS,
            ONLY_IF,
            FETCH_SIZE,
            SCENARIO,
            UPDATED_AT
        }

        final Type type;

        protected Option(Type type) {
            this.type = type;
        }

        /**
         * Creates a new Option object to add time-to-live to a mapper
         * operation. This is only valid for save operations.
         * <p/>
         * Note that this option is only available if using
         * {@link ProtocolVersion#V2} or above.
         *
         * @param ttl the TTL (in seconds).
         * @return the option.
         */
        public static Option ttl(int ttl) {
            return new Ttl(ttl);
        }

        /**
         * Creates a new Option object to add a timestamp to a mapper operation.
         * This is only valid for save and delete operations.
         * <p/>
         * Note that this option is only available if using
         * {@link ProtocolVersion#V2} or above.
         *
         * @param timestamp the timestamp (in microseconds).
         * @return the option.
         */
        public static Option timestamp(long timestamp) {
            return new Timestamp(timestamp);
        }

        /**
         * Creates a new Option object to add a consistency level value to a
         * mapper operation. This is valid for save, delete and get operations.
         * <p/>
         * Note that the consistency level can also be defined at the mapper
         * level, as a parameter of the
         * {@link org.everthrift.cassandra.com.datastax.driver.mapping.annotations.Table}
         * annotation (this is redundant for backward compatibility). This
         * option, whether defined on a specific call or as the default, will
         * always take precedence over the annotation.
         *
         * @param cl the {@link com.datastax.driver.core.ConsistencyLevel} to
         *           use for the operation.
         * @return the option.
         */
        public static Option consistencyLevel(ConsistencyLevel cl) {
            return new ConsistencyLevelOption(cl);
        }

        /**
         * Creates a new Option object to enable query tracing for a mapper
         * operation. This is valid for save, delete and get operations.
         *
         * @param enabled whether to enable tracing.
         * @return the option.
         */
        public static Option tracing(boolean enabled) {
            return new Tracing(enabled);
        }

        /**
         * Creates a new Option object to specify whether null entity fields
         * should be included in insert queries. This option is valid only for
         * save operations.
         * <p/>
         * If this option is not specified, it defaults to {@code true} (null
         * fields are saved).
         *
         * @param enabled whether to include null fields in queries.
         * @return the option.
         */
        public static Option saveNullFields(boolean enabled) {
            return new SaveNullFields(enabled);
        }

        public static Option ifNotExist() {
            return IfNotExist.instance;
        }

        public static Option ifExist() {
            return IfExist.instance;
        }

        public static Option onlyIf() {
            return OnlyIf.instance;
        }

        public static Option fetchSize(int fetchSize) {
            return new FetchSize(fetchSize);
        }

        public static Option scenario(EntityMapper.Scenario scenario) {
            return new Scenario(scenario);
        }

        public static Option updatedAt(long value) {
            return new UpdatedAt(value);
        }

        public Type getType() {
            return this.type;
        }

        abstract void appendTo(Insert insert);

        abstract void appendTo(Update update);

        abstract void appendTo(Delete.Options usings);

        abstract void addToPreparedStatement(BoundStatement bs, int i);

        abstract void checkValidFor(QueryType qt, MappingManager manager) throws IllegalArgumentException;

        abstract boolean isIncludedInQuery();

        static class Ttl extends Option {

            private int ttlValue;

            Ttl(int value) {
                super(Type.TTL);
                this.ttlValue = value;
            }

            @Override
            void appendTo(Insert insert) {
                insert.using(QueryBuilder.ttl(QueryBuilder.bindMarker()));
            }

            @Override
            void appendTo(Delete.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void addToPreparedStatement(BoundStatement bs, int i) {
                bs.setInt(i, this.ttlValue);
            }

            @Override
            void checkValidFor(QueryType qt, MappingManager manager) {
                checkArgument(!manager.isCassandraV1, "TTL option requires native protocol v2 or above");
                checkArgument(qt == QueryType.SAVE || qt == QueryType.UPDATE, "TTL option is only allowed in save or update queries");
            }

            @Override
            boolean isIncludedInQuery() {
                return true;
            }

            @Override
            void appendTo(Update update) {
                update.using(QueryBuilder.ttl(QueryBuilder.bindMarker()));
            }
        }

        static class Timestamp extends Option {

            private long tsValue;

            Timestamp(long value) {
                super(Type.TIMESTAMP);
                this.tsValue = value;
            }

            @Override
            void appendTo(Insert insert) {
                insert.using(QueryBuilder.timestamp(QueryBuilder.bindMarker()));
            }

            @Override
            void appendTo(Delete.Options usings) {
                usings.and(QueryBuilder.timestamp(QueryBuilder.bindMarker()));
            }

            @Override
            void checkValidFor(QueryType qt, MappingManager manager) {
                checkArgument(!manager.isCassandraV1, "Timestamp option requires native protocol v2 or above");
                checkArgument(qt == QueryType.SAVE || qt == QueryType.DEL || qt == QueryType.UPDATE,
                              "Timestamp option is only allowed in save and delete and update queries");
            }

            @Override
            void addToPreparedStatement(BoundStatement bs, int i) {
                bs.setLong(i, this.tsValue);
            }

            @Override
            boolean isIncludedInQuery() {
                return true;
            }

            @Override
            void appendTo(Update update) {
                update.using(QueryBuilder.timestamp(QueryBuilder.bindMarker()));
            }
        }

        static class ConsistencyLevelOption extends Option {

            private ConsistencyLevel cl;

            ConsistencyLevelOption(ConsistencyLevel cl) {
                super(Type.CL);
                this.cl = cl;
            }

            @Override
            void appendTo(Insert insert) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Delete.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Update update) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void addToPreparedStatement(BoundStatement bs, int i) {
                bs.setConsistencyLevel(cl);
            }

            @Override
            void checkValidFor(QueryType qt, MappingManager manager) {
                checkArgument(qt == QueryType.SAVE || qt == QueryType.UPDATE || qt == QueryType.DEL || qt == QueryType.GET,
                              "Consistency level option is only allowed in save, update, delete and get queries");
            }

            @Override
            boolean isIncludedInQuery() {
                return false;
            }
        }

        static class Tracing extends Option {

            private boolean tracing;

            Tracing(boolean tracing) {
                super(Type.TRACING);
                this.tracing = tracing;
            }

            @Override
            void appendTo(Insert insert) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Delete.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Update update) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void addToPreparedStatement(BoundStatement bs, int i) {
                if (this.tracing) {
                    bs.enableTracing();
                }
            }

            @Override
            void checkValidFor(QueryType qt, MappingManager manager) {
                checkArgument(qt == QueryType.SAVE || qt == QueryType.UPDATE || qt == QueryType.DEL || qt == QueryType.GET,
                              "Tracing option is only allowed in save, update, delete and get queries");
            }

            @Override
            boolean isIncludedInQuery() {
                return false;
            }

        }

        static class SaveNullFields extends Option {

            private boolean saveNullFields;

            SaveNullFields(boolean saveNullFields) {
                super(SAVE_NULL_FIELDS);
                this.saveNullFields = saveNullFields;
            }

            @Override
            void appendTo(Insert insert) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Delete.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Update update) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void addToPreparedStatement(BoundStatement bs, int i) {
                // nothing to do
            }

            @Override
            void checkValidFor(QueryType qt, MappingManager manager) {
                checkArgument(qt == QueryType.SAVE || qt == QueryType.UPDATE,
                              "SaveNullFields option is only allowed in save and update queries");
            }

            @Override
            boolean isIncludedInQuery() {
                return false;
            }
        }

        static class IfNotExist extends Option {

            private static final IfNotExist instance = new IfNotExist();

            IfNotExist() {
                super(Type.IF_NOT_EXISTS);
            }

            @Override
            void appendTo(Insert insert) {
                insert.ifNotExists();
            }

            @Override
            void appendTo(Delete.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Update update) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void addToPreparedStatement(BoundStatement bs, int i) {

            }

            @Override
            void checkValidFor(QueryType qt, MappingManager manager) throws IllegalArgumentException {
                checkArgument(qt == QueryType.SAVE, "IF NOT EXISTS option is only allowed in save queries");
            }

            @Override
            boolean isIncludedInQuery() {
                return true;
            }
        }

        static class IfExist extends Option {

            private static final IfExist instance = new IfExist();

            IfExist() {
                super(Type.IF_EXISTS);
            }

            @Override
            void appendTo(Insert insert) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Delete.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Update update) {
                update.where().ifExists();
            }

            @Override
            void addToPreparedStatement(BoundStatement bs, int i) {

            }

            @Override
            void checkValidFor(QueryType qt, MappingManager manager) throws IllegalArgumentException {
                checkArgument(qt == QueryType.UPDATE, "IF NOT EXISTS option is only allowed in update queries");
            }

            @Override
            boolean isIncludedInQuery() {
                return true;
            }
        }

        static class OnlyIf extends Option {

            private static final OnlyIf instance = new OnlyIf();

            OnlyIf() {
                super(Type.ONLY_IF);
            }

            @Override
            void appendTo(Insert insert) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Delete.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Update update) {

            }

            @Override
            void addToPreparedStatement(BoundStatement bs, int i) {

            }

            @Override
            void checkValidFor(QueryType qt, MappingManager manager) throws IllegalArgumentException {
                checkArgument(qt == QueryType.UPDATE, "ONLY_IF option is only allowed in update queries");
            }

            @Override
            boolean isIncludedInQuery() {
                return true;
            }
        }

        static class FetchSize extends Option {

            private int fetchSize;

            FetchSize(int fetchSize) {
                super(Type.FETCH_SIZE);
                this.fetchSize = fetchSize;
            }

            @Override
            void appendTo(Insert insert) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Delete.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Update update) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void addToPreparedStatement(BoundStatement bs, int i) {
                bs.setFetchSize(fetchSize);
            }

            @Override
            void checkValidFor(QueryType qt, MappingManager manager) throws IllegalArgumentException {
                checkArgument(qt == QueryType.GET_ALL, "FETCH_SIZE option is only allowed in GET_ALL queries");
            }

            @Override
            boolean isIncludedInQuery() {
                return false;
            }
        }

        static class Scenario extends Option {

            private EntityMapper.Scenario scenario;

            Scenario(EntityMapper.Scenario scenario) {
                super(Type.SCENARIO);
                this.scenario = scenario;
            }

            public EntityMapper.Scenario getScenario() {
                return scenario;
            }

            @Override
            void appendTo(Insert insert) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Delete.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Update update) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void addToPreparedStatement(BoundStatement bs, int i) {

            }

            @Override
            void checkValidFor(QueryType qt, MappingManager manager) throws IllegalArgumentException {

            }

            @Override
            boolean isIncludedInQuery() {
                return false;
            }
        }

        static class UpdatedAt extends Option {

            final long updatedAt;

            UpdatedAt(long updatedAt) {
                super(Type.UPDATED_AT);
                this.updatedAt = updatedAt;
            }

            public long getValue() {
                return updatedAt;
            }

            @Override
            void appendTo(Insert insert) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Delete.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Update update) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void addToPreparedStatement(BoundStatement bs, int i) {

            }

            @Override
            void checkValidFor(QueryType qt, MappingManager manager) throws IllegalArgumentException {
                checkArgument(qt == QueryType.UPDATE, "UpdatedAt option is only allowed in update queries");
            }

            @Override
            boolean isIncludedInQuery() {
                return false;
            }
        }

    }

    private static class MapperQueryKey {
        private final QueryType queryType;

        private final EnumSet<Option.Type> optionTypes;

        private final Set<ColumnMapper<?>> columns;

        MapperQueryKey(QueryType queryType, Set<ColumnMapper<?>> columnMappers, EnumMap<Option.Type, Option> options) {
            Preconditions.checkNotNull(queryType);
            Preconditions.checkNotNull(options);
            Preconditions.checkNotNull(columnMappers);
            this.queryType = queryType;
            this.columns = columnMappers;
            this.optionTypes = EnumSet.noneOf(Option.Type.class);
            for (Option opt : options.values()) {
                if (opt.isIncludedInQuery()) {
                    this.optionTypes.add(opt.type);
                }
            }
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other instanceof MapperQueryKey) {
                MapperQueryKey that = (MapperQueryKey) other;
                return this.queryType.equals(that.queryType) && this.optionTypes.equals(that.optionTypes)
                    && this.columns.equals(that.columns);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(queryType, optionTypes, columns);
        }
    }

    @Override
    public String toString() {
        return mapper.toString();
    }
}

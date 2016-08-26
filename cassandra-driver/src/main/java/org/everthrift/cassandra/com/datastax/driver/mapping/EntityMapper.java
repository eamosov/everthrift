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

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.datastax.driver.core.querybuilder.QueryBuilder.quote;

public abstract class EntityMapper<T> {

    public static enum ColumnScenario {
        COMMON,
        OPTIONAL
    }

    public static enum Scenario {
        COMMON {
            @Override
            boolean accept(ColumnScenario c) {
                return c == ColumnScenario.COMMON;
            }
        },
        ALL {
            @Override
            boolean accept(ColumnScenario c) {
                return true;
            }
        };

        abstract boolean accept(ColumnScenario c);
    }

    public final Class<T> entityClass;

    private final String keyspace;

    private final String table;

    protected ConsistencyLevel writeConsistency;

    protected ConsistencyLevel readConsistency;

    public final List<ColumnMapper<T>> partitionKeys = new ArrayList<ColumnMapper<T>>();

    public final List<ColumnMapper<T>> clusteringColumns = new ArrayList<ColumnMapper<T>>();

    public final List<ColumnMapper<T>> regularColumns = new ArrayList<ColumnMapper<T>>();

    private ColumnMapper<T> versionColumn;

    private ColumnMapper<T> updatedAtColumn;

    private Map<String, ColumnMapper<T>> allColumns = new HashMap<String, ColumnMapper<T>>();

    protected EntityMapper(Class<T> entityClass, String keyspace, String table, ConsistencyLevel writeConsistency,
                           ConsistencyLevel readConsistency) {
        this.entityClass = entityClass;
        this.keyspace = keyspace;
        this.table = table;
        this.writeConsistency = writeConsistency;
        this.readConsistency = readConsistency;
    }

    public String getKeyspace() {
        return quote(keyspace);
    }

    public String getTable() {
        return quote(table);
    }

    public int primaryKeySize() {
        return partitionKeys.size() + clusteringColumns.size();
    }

    public ColumnMapper<T> getColumnByFieldName(final String fieldName) {
        return allColumns.get(fieldName);
    }

    public ColumnMapper<T> getPrimaryKeyColumn(int i) {
        return i < partitionKeys.size() ? partitionKeys.get(i) : clusteringColumns.get(i - partitionKeys.size());
    }

    public void addColumns(List<ColumnMapper<T>> pks, List<ColumnMapper<T>> ccs, List<ColumnMapper<T>> rgs) {
        partitionKeys.addAll(pks);
        addToAllColumns(pks);

        clusteringColumns.addAll(ccs);
        addToAllColumns(ccs);

        regularColumns.addAll(rgs);
        addToAllColumns(rgs);
    }

    private void addToAllColumns(List<ColumnMapper<T>> cs) {
        cs.forEach(c -> {
            allColumns.put(c.getFieldName(), c);
        });
    }

    public void setVersionColumn(ColumnMapper<T> v) {
        versionColumn = v;
    }

    public void setUpdatedAtColumn(ColumnMapper<T> v) {
        updatedAtColumn = v;
    }

    public abstract T newEntity();

    public Set<ColumnMapper<T>> allColumns(Scenario scenario) {
        return ImmutableSet.copyOf(Collections2.filter(allColumns.values(), c -> (scenario.accept(c.columnScenario))));
    }

    public boolean isVersion(ColumnMapper<?> cm) {
        return versionColumn != null && cm == versionColumn;
    }

    public boolean isUpdatedAt(ColumnMapper<?> cm) {
        return updatedAtColumn != null && cm == updatedAtColumn;
    }

    public ColumnMapper<T> getUpdatedAtColumn() {
        return updatedAtColumn;
    }

    public ColumnMapper<T> getVersionColumn() {
        return versionColumn;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(String.format("[EntityMapper %s.%s <-> %s]\n", keyspace, table, entityClass.getCanonicalName()));
        sb.append("\twriteConsistency: " + writeConsistency + "\n");
        sb.append("\treadConsistency: " + readConsistency + "\n");
        if (versionColumn != null) {
            sb.append("\tversionColumn: " + versionColumn.getColumnName() + "\n");
        }

        sb.append("\tcolumns mapping:\n");

        for (int i = 0; i < this.primaryKeySize(); i++) {
            ColumnMapper<T> cm = getPrimaryKeyColumn(i);
            sb.append(String.format("\t\t%s %s <-> %s (%s)\n", cm.columnScenario, cm.getColumnNameUnquoted(), cm.getFieldName(),
                                    cm.getJavaType().toString()));
        }

        for (ColumnMapper<T> cm : regularColumns) {
            sb.append(String.format("\t\t%s %s <-> %s (%s)\n", cm.columnScenario, cm.getColumnNameUnquoted(), cm.getFieldName(),
                                    cm.getJavaType().toString()));
        }

        return sb.toString();
    }

    public interface Factory {
        public <T> EntityMapper<T> create(Class<T> entityClass, String keyspace, String table, ConsistencyLevel writeConsistency,
                                          ConsistencyLevel readConsistency);

        public <T> ColumnMapper<T> createColumnMapper(Class<T> componentClass, FieldDescriptor field, MappingManager mappingManager,
                                                      AtomicInteger columnCounter);
    }
}

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
package com.datastax.driver.mapping;

import static com.datastax.driver.core.querybuilder.QueryBuilder.quote;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.driver.core.ConsistencyLevel;

public abstract class EntityMapper<T> {

    public final Class<T> entityClass;
    private final String keyspace;
    private final String table;

    public final ConsistencyLevel writeConsistency;
    public final ConsistencyLevel readConsistency;

    public final List<ColumnMapper<T>> partitionKeys = new ArrayList<ColumnMapper<T>>();
    public final List<ColumnMapper<T>> clusteringColumns = new ArrayList<ColumnMapper<T>>();
    public final List<ColumnMapper<T>> regularColumns = new ArrayList<ColumnMapper<T>>();
    public ColumnMapper<T> versionColumn;
    
    private Map<String, ColumnMapper<T>> allColumns = new HashMap<String, ColumnMapper<T>>();

    protected EntityMapper(Class<T> entityClass, String keyspace, String table, ConsistencyLevel writeConsistency, ConsistencyLevel readConsistency) {
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

    public ColumnMapper<T> getColumnByFieldName(final String fieldName){
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
    
    private void addToAllColumns(List<ColumnMapper<T>> cs){
    	cs.forEach(c -> {
    		allColumns.put(c.getFieldName(), c);
    	});    	
    }

    public void setVersionColumn(ColumnMapper<T> v) {
    	versionColumn = v;
    }

    public abstract T newEntity();

    public Collection<ColumnMapper<T>> allColumns() {
        return allColumns.values();
    }
    
    public boolean isVersion(ColumnMapper<?> cm){
    	return versionColumn !=null && versionColumn.getColumnName().equals(cm.getColumnName());	
    }

    public interface Factory {
        public <T> EntityMapper<T> create(Class<T> entityClass, String keyspace, String table, ConsistencyLevel writeConsistency, ConsistencyLevel readConsistency);

        public <T> ColumnMapper<T> createColumnMapper(Class<T> componentClass, FieldDescriptor field, MappingManager mappingManager, AtomicInteger columnCounter);
    }
}

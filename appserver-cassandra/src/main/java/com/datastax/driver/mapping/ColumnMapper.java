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

import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.mapping.EntityMapper.ColumnScenario;
import com.google.common.reflect.TypeToken;

public abstract class ColumnMapper<T> {

    public enum Kind {PARTITION_KEY, CLUSTERING_COLUMN, REGULAR, COMPUTED}

    ;

    private final String columnName;
    private final String alias;
    protected final String fieldName;
    /**
     * The type of the Java field in the mapped class
     */
    protected final TypeToken<Object> fieldType;
    protected final Kind kind;
    protected final TypeCodec<Object> customCodec;
    protected final ColumnScenario columnScenario;

    protected ColumnMapper(FieldDescriptor field, AtomicInteger columnCounter) {
        this.columnName = field.getColumnName();
        this.alias = (columnCounter != null)
                ? FieldDescriptor.newAlias(field, columnCounter.incrementAndGet())
                : null;
        this.fieldName = field.getFieldName();
        this.fieldType = field.getFieldType();
        this.kind = field.getKind();
        this.customCodec = field.getCustomCodec();
        this.columnScenario = field.getColumnScenario();
    }

    public abstract Object getValue(T entity);

    public abstract void setValue(T entity, Object value);

    public String getColumnName() {
        return kind == Kind.COMPUTED
                ? columnName
                : Metadata.quote(columnName);
    }

    public String getAlias() {
        return alias;
    }

    public TypeCodec<Object> getCustomCodec() {
        return customCodec;
    }

    /**
     * The Java type of this column.
     */
    public TypeToken<Object> getJavaType() {
        return fieldType;
    }
    
    public String getColumnNameUnquoted(){
    	return columnName;
    }

	public String getFieldName() {
		return fieldName;
	}    
}

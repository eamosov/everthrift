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

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.protocol.TType;

import com.datastax.driver.core.ConsistencyLevel;

/**
 * An {@link EntityMapper} implementation that use reflection to read and write fields
 * of an entity.
 */
public class ThriftReflectionMapper<T> extends EntityMapper<T> {

	private static ThriftReflectionFactory factory = new ThriftReflectionFactory();

    private ThriftReflectionMapper(Class<T> entityClass, String keyspace, String table, ConsistencyLevel writeConsistency, ConsistencyLevel readConsistency) {
        super(entityClass, keyspace, table, writeConsistency, readConsistency);
    }

    public static Factory factory() {
        return factory;
    }

    @Override
    public T newEntity() {
        try {
            return entityClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Can't create an instance of " + entityClass.getName());
        }
    }

    private static class LiteralMapper<T> extends ColumnMapper<T> {

        private final Method readMethod;
        private final Method writeMethod;

        private LiteralMapper(FieldDescriptor field, PropertyDescriptor pd, AtomicInteger columnCounter) {
            super(field, columnCounter);
            this.readMethod = pd.getReadMethod();
            this.writeMethod = pd.getWriteMethod();
        }

        @Override
        public Object getValue(T entity) {
            try {
                return readMethod.invoke(entity);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Could not get field '" + fieldName + "'");
            } catch (Exception e) {
                throw new IllegalStateException("Unable to access getter for '" + fieldName + "' in " + entity.getClass().getName(), e);
            }
        }

        @Override
        public void setValue(Object entity, Object value) {
            try {
                writeMethod.invoke(entity, value);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Could not set field '" + fieldName + "' to value '" + value + "'");
            } catch (Exception e) {
                throw new IllegalStateException("Unable to access setter for '" + fieldName + "' in " + entity.getClass().getName(), e);
            }
        }
    }

    private static class ThriftMapper<T> extends ColumnMapper<T> {

        private final TFieldIdEnum fId;
        private final FieldValueMetaData md;

        private ThriftMapper(FieldDescriptor field, TFieldIdEnum fId, FieldValueMetaData md, AtomicInteger columnCounter) {
            super(field, columnCounter);
            this.fId = fId;
            this.md = md;
        }

        @Override
        public Object getValue(T entity) {
    		if (((TBase)entity).isSet(fId))
    			return ((TBase)entity).getFieldValue(fId);
    		else
    			return null;
        }

    	private boolean isBox(Class box, Class primitive){
    		
    		return (box.equals(Byte.class) && primitive.equals(Byte.TYPE)) ||
    				(box.equals(Double.class) && primitive.equals(Double.TYPE)) ||
    				(box.equals(Short.class) && primitive.equals(Short.TYPE)) ||
    				(box.equals(Integer.class) && primitive.equals(Integer.TYPE)) ||
    				(box.equals(Long.class) && primitive.equals(Long.TYPE));
    	}

        @Override
        public void setValue(Object entity, Object value) {
        	
    		if (value!=null && !fieldType.getRawType().isAssignableFrom(value.getClass()) && !isBox(value.getClass(), fieldType.getRawType())){

    			if (value instanceof Number){
    				switch(md.type){
    				case TType.BYTE:
    					value = ((Number)value).byteValue();
    					break;
    				case TType.DOUBLE:
    					value = ((Number)value).doubleValue();
    					break;
    				case TType.I16:
    					value = ((Number)value).shortValue();
    					break;
    				case TType.I32:
    					value = ((Number)value).intValue();
    					break;
    				case TType.I64:
    					value = ((Number)value).longValue();
    					break;
    				}
    			}
    		}else if (value !=null && value.getClass().equals(byte[].class) && fieldType.getRawType().equals(byte[].class)){
    			//В thrift setFieldValue ожидает ByteBuffer для типа binary
    			value = ByteBuffer.wrap(Arrays.copyOf((byte[])value, ((byte[])value).length));
    		}
    		
    		((TBase)entity).setFieldValue(fId, value);
        }
    }

    private static class ThriftReflectionFactory implements Factory {

        @Override
        public <T> EntityMapper<T> create(Class<T> entityClass, String keyspace, String table, ConsistencyLevel writeConsistency, ConsistencyLevel readConsistency) {
            return new ThriftReflectionMapper<T>(entityClass, keyspace, table, writeConsistency, readConsistency);
        }

        @Override
        public <T> ColumnMapper<T> createColumnMapper(Class<T> entityClass, FieldDescriptor field, MappingManager mappingManager, AtomicInteger columnCounter) {
            final String fieldName = field.getFieldName();
            try {

                for (Class<?> udt : TypeMappings.findUDTs(field.getFieldType().getType()))
                    mappingManager.getUDTCodec(udt);                

                if (TBase.class.isAssignableFrom(entityClass)){
                	
            		Map<TFieldIdEnum, FieldMetaData> map = null;
            		Class<? extends TBase> thriftClass = (Class)entityClass;
            		do{
            			try{
            				map = (Map)FieldMetaData.getStructMetaDataMap(thriftClass);
            				if (map !=null)
            					break;
            			}catch(Exception e){
            				map = null;
            			}
            			thriftClass = (Class<? extends TBase>)thriftClass.getSuperclass();
            		}while(thriftClass !=null);
            		
            		if (map == null)
            			throw new RuntimeException("cound't final FieldMetaData for " + entityClass.getCanonicalName());
            		
            		for (Map.Entry<TFieldIdEnum, FieldMetaData> e : map.entrySet()){            			
            			if (e.getKey().getFieldName().equalsIgnoreCase(field.getFieldName())){
            				return new ThriftMapper<T>(field, e.getKey(), e.getValue().valueMetaData, columnCounter);
            			}
            		}
                }
                
                PropertyDescriptor pd = new PropertyDescriptor(fieldName, entityClass);

                return new LiteralMapper<T>(field, pd, columnCounter);
            } catch (IntrospectionException e) {
                throw new IllegalArgumentException("Cannot find matching getter and setter for field '" + fieldName + "'");
            }
        }
    }
}

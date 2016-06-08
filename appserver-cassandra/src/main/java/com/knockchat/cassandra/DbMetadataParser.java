package com.knockchat.cassandra;

import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UserType;
import com.datastax.driver.mapping.AnnotationChecks;
import com.datastax.driver.mapping.ColumnMapper;
import com.datastax.driver.mapping.EntityMapper;
import com.datastax.driver.mapping.EntityMapper.ColumnScenario;
import com.datastax.driver.mapping.EntityMapper.Factory;
import com.datastax.driver.mapping.EntityParser;
import com.datastax.driver.mapping.FieldDescriptor;
import com.datastax.driver.mapping.MappedUDTCodec;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Computed;
import com.datastax.driver.mapping.annotations.Defaults;
import com.datastax.driver.mapping.annotations.Scenario;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Transient;
import com.datastax.driver.mapping.annotations.UDT;
import com.datastax.driver.mapping.annotations.Version;
import com.google.common.base.CaseFormat;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.knockchat.thrift.TBaseHasModel;
import com.knockchat.utils.ClassUtils;

public class DbMetadataParser implements EntityParser {
	
	private static final Logger log = LoggerFactory.getLogger(DbMetadataParser.class);
	
	public static final DbMetadataParser INSTANCE = new DbMetadataParser();
	
	private static <T extends Annotation> T getAnnotation(Class entityClass, PropertyDescriptor pd, Class<T> annotationClass) {
		T a = pd.getReadMethod().getAnnotation(annotationClass);
		
		if ( a == null)
			a = pd.getWriteMethod().getAnnotation(annotationClass);
		
		if (a == null){			
			try {
				final Field f = entityClass.getDeclaredField(pd.getName());
				if (f !=null)
					a = f.getAnnotation(annotationClass);

			} catch (NoSuchFieldException e) {
			} catch (SecurityException e) {
			}
		}
		
		return a;
	}
	
    private static TypeCodec<Object> customCodec(Class entityClass, PropertyDescriptor pd) {
        Class<? extends TypeCodec<?>> codecClass = getCodecClass(entityClass, pd);

        if (codecClass.equals(Defaults.NoCodec.class))
            return null;

        try {
            @SuppressWarnings("unchecked")
            TypeCodec<Object> instance = (TypeCodec<Object>) codecClass.newInstance();
            return instance;
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                    "Cannot create an instance of custom codec %s for field %s",
                    codecClass, pd.getName()
            ), e);
        }
    }

    private static Class<? extends TypeCodec<?>> getCodecClass(Class entityClass, PropertyDescriptor pd) {
        Column column = getAnnotation(entityClass, pd, Column.class);
        if (column != null)
            return column.codec();

        com.datastax.driver.mapping.annotations.Field udtField = getAnnotation(entityClass, pd, com.datastax.driver.mapping.annotations.Field.class);
        if (udtField != null)
            return udtField.codec();

        return Defaults.NoCodec.class;
    }


	public <T> EntityMapper<T> parseEntity(Class<T> entityClass, Factory factory, MappingManager mappingManager) {
				
		final Table table = AnnotationChecks.getTypeAnnotation(Table.class, entityClass);

        String ksName = table.caseSensitiveKeyspace() ? table.keyspace() : table.keyspace().toLowerCase();
        final String tableName = table.caseSensitiveTable() ? table.name() : table.name().toLowerCase();

        final ConsistencyLevel writeConsistency = table.writeConsistency().isEmpty() ? null : ConsistencyLevel.valueOf(table.writeConsistency().toUpperCase());
        final ConsistencyLevel readConsistency = table.readConsistency().isEmpty() ? null : ConsistencyLevel.valueOf(table.readConsistency().toUpperCase());

        if (Strings.isNullOrEmpty(table.keyspace())) {
            ksName = mappingManager.getSession().getLoggedKeyspace();
            if (Strings.isNullOrEmpty(ksName))
                throw new IllegalArgumentException(String.format(
                        "Error creating mapper for class %s, the @%s annotation declares no default keyspace, and the session is not currently logged to any keyspace",
                        entityClass.getSimpleName(),
                        Table.class.getSimpleName()
                ));
        }

        final EntityMapper<T> mapper = factory.create(entityClass, ksName, tableName, writeConsistency, readConsistency);
        
		final Map<String, PropertyDescriptor> entityProps = ClassUtils.getPropertyDescriptors(entityClass); 
		
		final TableMetadata tableMetadata = mappingManager.getSession().getCluster().getMetadata().getKeyspace(ksName).getTable(tableName);

		final List<ColumnMapper<T>> pks = new ArrayList<ColumnMapper<T>>();
		final List<ColumnMapper<T>> ccs = new ArrayList<ColumnMapper<T>>();
		final List<ColumnMapper<T>> rgs = new ArrayList<ColumnMapper<T>>();
		ColumnMapper<T> versionField = null;
		ColumnMapper<T> updatedAtField = null;
		final AtomicInteger columnCounter = mappingManager.isCassandraV1 ? null : new AtomicInteger(0);
		
		final Set<String> keys = new HashSet<String>();
		
		for (ColumnMetadata cm: tableMetadata.getPartitionKey()){
			
			PropertyDescriptor javaProp = entityProps.get(cm.getName());
			if (javaProp == null)
				javaProp = entityProps.get(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, cm.getName()));
			
			if (javaProp == null)
				throw new IllegalArgumentException("Coudn't find partition key '" + cm.getName() + "' in java class " + entityClass.getCanonicalName());

			final FieldDescriptor fd = new FieldDescriptor(cm.getName(), javaProp.getName(), ColumnMapper.Kind.PARTITION_KEY, ColumnScenario.COMMON, (TypeToken)TypeToken.of(javaProp.getReadMethod().getGenericReturnType()), customCodec(entityClass, javaProp));
			pks.add(factory.createColumnMapper(entityClass, fd, mappingManager, columnCounter));
			keys.add(javaProp.getName());
		}
		
		for (ColumnMetadata cm: tableMetadata.getClusteringColumns()){
			
			PropertyDescriptor javaProp = entityProps.get(cm.getName());
			if (javaProp == null)
				javaProp = entityProps.get(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, cm.getName()));
			
			if (javaProp == null)
				throw new IllegalArgumentException("Coudn't find partition key '" + cm.getName() + "' in java class " + entityClass.getCanonicalName());

			final FieldDescriptor fd = new FieldDescriptor(cm.getName(), javaProp.getName(), ColumnMapper.Kind.CLUSTERING_COLUMN, ColumnScenario.COMMON, (TypeToken)TypeToken.of(javaProp.getReadMethod().getGenericReturnType()), customCodec(entityClass, javaProp));
			ccs.add(factory.createColumnMapper(entityClass, fd, mappingManager, columnCounter));
			keys.add(javaProp.getName());
		}
		
		final Map<String, ColumnMetadata> columnsByName = Maps.uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);
		
		for (PropertyDescriptor javaProp: entityProps.values()){
			
			if (keys.contains(javaProp.getName()) || javaProp.getReadMethod() == null || javaProp.getWriteMethod() == null)
				continue;
			
            if (getAnnotation(entityClass, javaProp, Transient.class) != null)            	
                continue;
			
			final Column aColumn = getAnnotation(entityClass, javaProp, Column.class);
			ColumnMetadata columnMetadata;
			if (aColumn !=null && !aColumn.name().isEmpty()){
				columnMetadata = columnsByName.get(aColumn.name());
				if (columnMetadata == null)
					throw new IllegalArgumentException("Coudn't find column '" + aColumn.name() + "' for map " + entityClass.getCanonicalName() + "." + javaProp.getName());
			}else{
				columnMetadata = columnsByName.get(javaProp.getName());
				
				if (columnMetadata == null)
					columnMetadata = columnsByName.get(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, javaProp.getName()));
			}
			
			if (columnMetadata == null)
				continue;
			
            final boolean isComputed = getAnnotation(entityClass, javaProp, Computed.class) != null;
            if (mappingManager.isCassandraV1 && isComputed)
                throw new UnsupportedOperationException("Computed fields are not supported with native protocol v1");
			
            final TypeToken<Object> fieldType = (TypeToken)TypeToken.of(javaProp.getReadMethod().getGenericReturnType());
            final TypeCodec<Object> customCodec = customCodec(entityClass, javaProp);
            
            Scenario scenario = getAnnotation(entityClass, javaProp, Scenario.class);

			final FieldDescriptor fd = new FieldDescriptor(columnMetadata.getName(), javaProp.getName(),
					isComputed ? ColumnMapper.Kind.COMPUTED : ColumnMapper.Kind.REGULAR,
					scenario == null ? EntityMapper.ColumnScenario.COMMON : scenario.value(), fieldType, customCodec);
			 
			final ColumnMapper<T> columnMapper = factory.createColumnMapper(entityClass, fd, mappingManager, columnCounter); 
			rgs.add(columnMapper);
            			
			if (getAnnotation(entityClass, javaProp, Version.class) !=null){
				
				if (versionField !=null)
					throw new IllegalArgumentException(String.format("Confliction in version fields: %s, %s", versionField.getColumnName(), columnMapper.getColumnName()));
								
				if (!table.version().isEmpty())
					throw new IllegalArgumentException(String.format("@Version annotation not allowed while @Table has parameter 'version'"));

				versionField = columnMapper;
			}else if (table.version().equalsIgnoreCase(javaProp.getName())){
				versionField = columnMapper;
			}
			
			if (javaProp.getName().equals("updatedAt")){
				updatedAtField = columnMapper;
			}
		}
		
        mapper.addColumns(pks,ccs,rgs);
        mapper.setVersionColumn(versionField);
        mapper.setUpdatedAtColumn(updatedAtField);
        
        return mapper;		
	}

    public <T> MappedUDTCodec<T> parseUDT(Class<T> udtClass, EntityMapper.Factory factory, MappingManager mappingManager) {
        UDT udt = AnnotationChecks.getTypeAnnotation(UDT.class, udtClass);

        String ksName = udt.caseSensitiveKeyspace() ? udt.keyspace() : udt.keyspace().toLowerCase();
        String udtName = udt.caseSensitiveType() ? Metadata.quote(udt.name()) : udt.name().toLowerCase();

        if (Strings.isNullOrEmpty(udt.keyspace())) {
            ksName = mappingManager.getSession().getLoggedKeyspace();
            if (Strings.isNullOrEmpty(ksName))
                throw new IllegalArgumentException(String.format(
                        "Error creating UDT codec for class %s, the @%s annotation declares no default keyspace, and the session is not currently logged to any keyspace",
                        udtClass.getSimpleName(),
                        UDT.class.getSimpleName()
                ));
        }

        final UserType userType = mappingManager.getSession().getCluster().getMetadata().getKeyspace(ksName).getUserType(udtName);
        
		final Map<String, PropertyDescriptor> entityProps = ClassUtils.getPropertyDescriptors(udtClass);         
		final Map<String, ColumnMapper<T>> columnMappers = Maps.newHashMap();
		
		for (String udtFieldName: userType.getFieldNames()){
			
			PropertyDescriptor javaProp = entityProps.get(udtFieldName);
			if (javaProp == null)
				javaProp = entityProps.get(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, udtFieldName));
			
			if (javaProp == null){
				log.warn("Coudn't find eny fields to map {}.{}", udtName, udtFieldName);
				continue;
			}
			
            if (getAnnotation(udtClass, javaProp, Transient.class) != null)            	
                continue;
            
            final TypeToken<Object> fieldType = (TypeToken)TypeToken.of(javaProp.getReadMethod().getGenericReturnType());
            final TypeCodec<Object> customCodec = customCodec(udtClass, javaProp);

			final FieldDescriptor fd = new FieldDescriptor(udtFieldName, javaProp.getName(), ColumnMapper.Kind.REGULAR, EntityMapper.ColumnScenario.COMMON, fieldType, customCodec);            
			final ColumnMapper<T> m = factory.createColumnMapper(udtClass, fd, mappingManager, null);
			
			columnMappers.put(m.getColumnName(), m);
		}
		
		return new MappedUDTCodec<T>(userType, udtClass, TBaseHasModel.getModel((Class)udtClass) !=null ? (Class)TBaseHasModel.getModel((Class)udtClass) : udtClass, columnMappers, mappingManager);
    }
	    
}

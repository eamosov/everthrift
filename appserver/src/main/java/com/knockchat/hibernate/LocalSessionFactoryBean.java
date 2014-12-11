package com.knockchat.hibernate;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;
import java.util.Map;
import java.util.Properties;

import org.apache.thrift.TEnum;
import org.hibernate.SessionFactory;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.Mappings;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Component;
import org.hibernate.mapping.PrimaryKey;
import org.hibernate.mapping.Property;
import org.hibernate.mapping.PropertyGeneration;
import org.hibernate.mapping.RootClass;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.mapping.Table;
import org.hibernate.mapping.Value;
import org.hibernate.type.BigDecimalType;
import org.hibernate.type.BigIntegerType;
import org.hibernate.type.BooleanType;
import org.hibernate.type.ByteType;
import org.hibernate.type.CharacterType;
import org.hibernate.type.DateType;
import org.hibernate.type.DoubleType;
import org.hibernate.type.FloatType;
import org.hibernate.type.IntegerType;
import org.hibernate.type.LongType;
import org.hibernate.type.NumericBooleanType;
import org.hibernate.type.ShortType;
import org.hibernate.type.StringType;
import org.hibernate.type.TimeType;
import org.hibernate.type.TimestampType;
import org.hibernate.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.orm.hibernate4.LocalSessionFactoryBuilder;

import com.google.common.collect.Maps;
import com.knockchat.hibernate.model.types.DateTypeFactory;
import com.knockchat.hibernate.model.types.TEnumTypeFactory;

public class LocalSessionFactoryBean extends org.springframework.orm.hibernate4.LocalSessionFactoryBean {

    public static final Map<Class,Map<String,Type>> classMappings = Maps.newHashMap();

    public static final String DEFAULT_ACCESSOR ="com.knockchat.hibernate.ThriftModelPropertyAccessor";
    public static final String KEY_CLASS_NAME = "$Key";
    public static final boolean LAZY = false;

    private static final Logger LOG = LoggerFactory.getLogger(LocalSessionFactoryBean.class);

    private MetaDataProvider metaDataProvider;

    @Override
    protected SessionFactory buildSessionFactory(LocalSessionFactoryBuilder sfb) {
        addMappings(sfb);
        return super.buildSessionFactory(sfb);
    }

    protected void addMappings(LocalSessionFactoryBuilder sfb){
        final Mappings mappings = sfb.createMappings();
        mappings.setDefaultAccess(DEFAULT_ACCESSOR);
        for (com.knockchat.hibernate.model.Table table :metaDataProvider.getTableModels()){
            if (table.isValid()){
                RootClass clazz = createTableMapping(mappings, table);
                if(clazz != null) {
                    mappings.addClass(clazz);
                    mappings.addImport(clazz.getEntityName(), clazz.getEntityName());
                }
            } else{
                LOG.error("Skip invalid table mapping {} ", table.getTableName());
            }

        }
    }

    private RootClass createTableMapping(Mappings mappings, com.knockchat.hibernate.model.Table tableModel) {
        final Table tab = mappings.addTable(tableModel.getSchema(), null,tableModel.getTableName(), null, false);
        mappings.addTableBinding(tableModel.getSchema(), null, tableModel.getTableName(), tableModel.getTableName(), null);
        final RootClass clazz = new RootClass();
        clazz.setDynamicUpdate(true);
        clazz.setDynamicInsert(true);
        
        final String secondLevelCache = getHibernateProperties().getProperty("hibernate.cache.use_second_level_cache", "false");
        
        if (secondLevelCache.equalsIgnoreCase("true"))
        	clazz.setCacheConcurrencyStrategy(AccessType.NONSTRICT_READ_WRITE.getExternalName());

        clazz.setEntityName(tableModel.getJavaClass().getName());
        clazz.setJpaEntityName(tableModel.getJavaClass().getName());
        clazz.setNodeName(tableModel.getJavaClass().getName());
        if (tableModel.getJavaClass() != null) {
            classMappings.put(tableModel.getJavaClass(),Maps.<String,Type>newHashMap());
            clazz.setClassName(tableModel.getJavaClass().getName());
            clazz.setProxyInterfaceName(tableModel.getJavaClass().getName());
        }
        clazz.setLazy(LAZY);
        clazz.setTable(tab);

        if ( tableModel.getPrimaryKey().getColumnNames().size() > 1 )
            createPKCompositeWODedicatedPropery(mappings, tableModel, clazz, tab);
        else
            createPKSingle(mappings, tableModel, clazz, tab);

        for(com.knockchat.hibernate.model.Column columnModel : tableModel.getColumnsByName().values()) {
            if (tableModel.getPrimaryKey().getColumnNames().contains(columnModel.getColumnName()))
                continue; //skip already added PK column
            Column col = createColumn(mappings, tab, columnModel, tableModel, true);
            if(col != null) {
                clazz.addProperty(createProperty(columnModel, col.getValue(),true, true));
            }
        }
        return clazz;
    }

    protected void createPKSingle(Mappings mappings, com.knockchat.hibernate.model.Table mdTable, RootClass clazz, Table tab) {
        com.knockchat.hibernate.model.Column pkcol = mdTable.getColumnsByName().get(mdTable.getPrimaryKey().getColumnNames().get(0));
        final PrimaryKey primaryKey = new PrimaryKey();
        primaryKey.setName(mdTable.getPrimaryKey().getPrimaryKeyName());
        primaryKey.setTable(tab);
        tab.setPrimaryKey(primaryKey);

        Column col = createColumn(mappings, tab, pkcol, mdTable, true);

        if (col == null) {
            LOG.error("Skipping primary key");
            return;
        }

        SimpleValue id = (SimpleValue) col.getValue();
        if (pkcol.isAutoincrement() || mdTable.isView()) //для view  выставляем автоинкремент
        	id.setIdentifierGeneratorStrategy("identity");
        else
        	id.setIdentifierGeneratorStrategy("assigned");
        	
        id.setNullValue("undefined");

        tab.getPrimaryKey().addColumn(col);

        Property prop = createProperty(pkcol, id, true, true);
        clazz.addProperty(prop);

        prop.setInsertable(false);
        prop.setUpdateable(false);        
        tab.setIdentifierValue(id);
        clazz.setIdentifier(id);
        clazz.setIdentifierProperty(prop);
    }

    protected void createPKComposite(Mappings mappings, com.knockchat.hibernate.model.Table tableModel, RootClass clazz, Table tab) {

        PrimaryKey primaryKey = new PrimaryKey();
        primaryKey.setName(tableModel.getPrimaryKey().getColumnNames().get(0));
        primaryKey.setTable(tab);

        clazz.setEmbeddedIdentifier(true);
        Component component = new Component(mappings, clazz);
        component.setDynamic(tableModel.getJavaClass()==null);
        String name;
        name = tableModel.getTableName();

        component.setRoleName(name + ".id");
        component.setEmbedded(true);
        component.setKey(true);
        component.setNullValue("undefined");

        if (!component.isDynamic()){
            component.setComponentClassName(tableModel.getJavaClass().getCanonicalName());
        }

        boolean hasErrors = false;
        for (String  columnName : tableModel.getPrimaryKey().getColumnNames()) {
            com.knockchat.hibernate.model.Column pkColumn = tableModel.getColumnsByName().get(columnName);
            if (pkColumn == null ) {
                throw new InternalError("Not find column for PK column name");
            }

            Column col = createColumn(mappings, tab, pkColumn, tableModel, true);

            hasErrors = col == null || hasErrors;

            if(col != null) {
                primaryKey.addColumn(col);
                Property prop = createProperty( pkColumn , col.getValue(), false, false);
                prop.setCascade("none");
                prop.setPersistentClass(clazz);
                component.addProperty(prop);
            }
        }
        if (hasErrors) {
            LOG.error("Skipping primary key");
            return;
        }
        tab.setIdentifierValue(component);
        clazz.setIdentifier(component);
        clazz.setDiscriminatorValue(name);
        tab.setPrimaryKey(primaryKey);
    }

    protected void createPKCompositeWithDedicatedPropery(Mappings mappings,
                                     com.knockchat.hibernate.model.Table tableModel,
                                     RootClass clazz, Table tab) {


        PrimaryKey primaryKey = new PrimaryKey();
        primaryKey.setName(tableModel.getPrimaryKey().getColumnNames().get(0));
        primaryKey.setTable(tab);

        clazz.setEmbeddedIdentifier(false);
        Component component = new Component(mappings, clazz);
        component.setDynamic(tableModel.getJavaClass()==null);
        String name = tableModel.getTableName();

        //component.setRoleName(name + ".id");
        component.setEmbedded(false);
        component.setNodeName("id");
        component.setKey(true);
        component.setNullValue("undefined");

        if (!component.isDynamic()){
            component.setComponentClassName(tableModel.getJavaClass().getCanonicalName() + "$Key");
        }

        boolean hasErrors = false;
        for (String  columnName : tableModel.getPrimaryKey().getColumnNames()) {
            com.knockchat.hibernate.model.Column pkColumn = tableModel.getColumnsByName().get(columnName);
            if (pkColumn == null ) {
                throw new InternalError("Not find column for PK column name");
            }

            Column col = createColumn(mappings, tab, pkColumn, tableModel, true);

            hasErrors = col == null || hasErrors;

            if(col != null) {
                primaryKey.addColumn(col);
                Property prop = createProperty( pkColumn , col.getValue(),true, true);
                Property classProp = createProperty( pkColumn , col.getValue(), false, false);
                prop.setNodeName(col.getName());
                prop.setGeneration(PropertyGeneration.NEVER);
                //prop.setCascade("none");
                //prop.setPropertyAccessorName(DEFAULT_ACCESSOR);
                //prop.setPersistentClass(clazz);
                clazz.addProperty(classProp);
                component.addProperty(prop);
            }
        }
        if (hasErrors) {
            LOG.error("Skipping primary key");
            return;
        }

        tab.setIdentifierValue(component);
        clazz.setIdentifier(component);
        clazz.setDiscriminatorValue(name);

        Property idprop = new Property();
        idprop.setName("id");
        idprop.setValue(component);
                    //idprop.setPropertyAccessorName(DEFAULT_ACCESSOR);
        clazz.setIdentifierProperty(idprop);
        tab.setPrimaryKey(primaryKey);
    }

    protected void createPKCompositeWODedicatedPropery(Mappings mappings, com.knockchat.hibernate.model.Table tableModel, RootClass clazz, Table tab) {
        PrimaryKey primaryKey = new PrimaryKey();
        primaryKey.setName(tableModel.getPrimaryKey().getPrimaryKeyName());
        primaryKey.setTable(tab);

        clazz.setEmbeddedIdentifier(false);

        Component component = new Component(mappings, clazz);
        component.setDynamic(false);
        component.setEmbedded(true);
        component.setNodeName("id");
        component.setKey(true);
        component.setNullValue("undefined");
        component.setIdentifierGeneratorStrategy("assigned");
        component.setComponentClassName(tableModel.getJavaClass().getCanonicalName().concat(KEY_CLASS_NAME));

        Component identifierMapper = new Component(mappings, clazz);
        identifierMapper.setDynamic(false);
        identifierMapper.setEmbedded(true);
        identifierMapper.setNodeName("_identifierMapper");
        identifierMapper.setKey(false);
        identifierMapper.setIdentifierGeneratorStrategy("assigned");
        identifierMapper.setComponentClassName(tableModel.getJavaClass().getCanonicalName());

        boolean hasErrors = false;
        for (String  columnName : tableModel.getPrimaryKey().getColumnNames()) {
            com.knockchat.hibernate.model.Column pkColumnModel = tableModel.getColumnsByName().get(columnName);
            if (pkColumnModel == null )
                throw new InternalError("Not find column for PK column name");

            Column col = createColumn(mappings, tab, pkColumnModel, tableModel, true);
            Column colForIdComponent = createColumn(mappings, tab, pkColumnModel, tableModel, false);
            hasErrors = col == null || hasErrors;

            if(col != null) {
                primaryKey.addColumn(col);
                Property prop = createProperty( pkColumnModel , col.getValue(),true, true);
                Property propForIdComponent = createProperty( pkColumnModel , colForIdComponent.getValue(),true, true);
                prop.setPropertyAccessorName(DEFAULT_ACCESSOR);
                identifierMapper.addProperty(propForIdComponent);
                component.addProperty(prop);
            }
        }
        if (hasErrors) {
            LOG.error("Skipping primary key");
            return;
        }
        tab.setIdentifierValue(component);
        clazz.setIdentifier(component);
        clazz.setDiscriminatorValue(tableModel.getTableName());

        Property idprop = new Property();
        idprop.setUpdateable(false);
        idprop.setInsertable(false);
        idprop.setNodeName("id");
        idprop.setName("_identifierMapper");
        idprop.setGeneration(PropertyGeneration.NEVER);
        idprop.setValue(identifierMapper);
        idprop.setPropertyAccessorName("embedded");

        clazz.setIdentifierMapper(identifierMapper);
        clazz.setDeclaredIdentifierMapper(identifierMapper);
        clazz.addProperty(idprop);
        tab.setPrimaryKey(primaryKey);
    }

    protected Column createColumn(Mappings mappings,
                                  Table tab,
                                  com.knockchat.hibernate.model.Column columnModel,
                                  com.knockchat.hibernate.model.Table tableModel, boolean bindToTable) {
        if (!columnModel.isValid()) return null;
        Column col = new Column();
        col.setName(columnModel.getColumnName());
        col.setLength(columnModel.getLength());
        col.setPrecision(columnModel.getLength());
        col.setScale(columnModel.getScale());
        col.setNullable(true);
        String columnType = columnModel.getColumnType();
        int jdbcType = columnModel.getJdbcType();

        col.setSqlTypeCode(jdbcType);
        col.setSqlType(columnType);

        SimpleValue value = new SimpleValue(mappings, tab);
        if (!setHibernateType(value,columnModel,col,columnModel.getJavaClass(),jdbcType))
            return null;
        value.addColumn(col);
        if (bindToTable) {
            tab.addColumn(col);
            mappings.addColumnBinding(columnModel.getColumnName(), col, tab);
            classMappings.get(tableModel.getJavaClass()).put(columnModel.getColumnName(), value.getType());
        }
        return col;
    }

    protected Property createProperty(com.knockchat.hibernate.model.Column columnModel, Value value, boolean insertable, boolean updatable) {
        Property prop = new Property();
        prop.setName(columnModel.getPropertyName());
        prop.setNodeName(columnModel.getPropertyName());
        prop.setValue(value);
        prop.setInsertable(insertable);
        prop.setUpdateable(updatable);
        prop.setPropertyAccessorName(DEFAULT_ACCESSOR);
        return prop;
    }




    public boolean setHibernateType(SimpleValue value,
                                    com.knockchat.hibernate.model.Column columnModel,
                                    Column col,
                                    Class javaType,
                                    final int jdbcType) {
    	
    	LOG.debug("{}.{} {} <-> {}", new Object[]{value.getTable().getName(), col.getName(), javaType, jdbcType});    	
    	
        String typeName;
        Properties typeParams = null;
        if(javaType == null) {
            return false;
        }
        if (javaType == Long.class || javaType == long.class) {            
            switch(jdbcType){
            	case Types.TIMESTAMP:
            		col.setCustomRead(col.getName()+"::bigint");
            		col.setCustomWrite("?::timestamp with time zone");
            		break;
            	default:
            }
            typeName = LongType.INSTANCE.getName();
        } else if (javaType == Short.class || javaType == short.class) {
            typeName = ShortType.INSTANCE.getName();
        } else if (javaType == Integer.class || javaType == int.class ) {
            typeName = IntegerType.INSTANCE.getName();
        } else if (javaType == Byte.class || javaType == byte.class ) {
            typeName = ByteType.INSTANCE.getName();
        } else if (javaType == Float.class || javaType == float.class) {
            typeName = FloatType.INSTANCE.getName();
        } else if (javaType == Double.class || javaType == double.class ) {
            typeName = DoubleType.INSTANCE.getName();
        } else if (javaType == Character.class || javaType == char.class) {
            typeName = CharacterType.INSTANCE.getName();
        } else if (javaType == String.class) {
            switch(jdbcType){
                case Types.TIMESTAMP:
                    col.setCustomRead(col.getName()+"::text");
                    col.setCustomWrite("?::timestamp");
                    break;
                case Types.OTHER:
                    if (columnModel.getColumnType().equals("geography")) {
                        col.setCustomRead(col.getName() + "::text");
                        col.setCustomWrite("?::geography");
                        break;
                    } else if (columnModel.getColumnType().equals("inet")){
                        col.setCustomRead(col.getName() + "::text");
                        col.setCustomWrite("?::inet");
                        break;
                    }
            }
            typeName = StringType.INSTANCE.getName();
        } else if (java.util.Date.class.isAssignableFrom(javaType)) {
            switch (jdbcType) {
                case Types.DATE:
                    typeName = DateType.INSTANCE.getName();
                    break;
                case Types.TIME:
                    typeName = TimeType.INSTANCE.getName();
                    break;
                case Types.TIMESTAMP:
                    typeName = TimestampType.INSTANCE.getName();
                    break;
                default:
                    typeName = null;
            }
        } else if (javaType == Boolean.class || javaType == boolean.class) {
            if(jdbcType == Types.BIT || jdbcType == Types.BOOLEAN) {
                typeName = BooleanType.INSTANCE.getName();
            } else if(jdbcType == Types.NUMERIC || jdbcType == Types.DECIMAL || jdbcType == Types.INTEGER ||
                    jdbcType == Types.SMALLINT || jdbcType == Types.TINYINT || jdbcType == Types.BIGINT) {
                typeName = NumericBooleanType.INSTANCE.getName();
            }
//            else if(jdbcType == Types.CHAR || jdbcType == Types.VARCHAR) {
//                typeName = StringBooleanType.class.getName();
//                typeParams = new Properties();
//                typeParams.setProperty("true", trueString != null ? trueString : StringBooleanType.NULL);
//                typeParams.setProperty("false", falseString != null ? falseString : StringBooleanType.NULL);
//                typeParams.setProperty("sqlType", String.valueOf(jdbcType));
//            }
            else {
                typeName = null;
            }
        } else if (javaType == BigDecimal.class) {
            typeName = BigDecimalType.INSTANCE.getName();
        } else if (javaType == BigInteger.class) {
            typeName = BigIntegerType.INSTANCE.getName();
        } else if (javaType == byte[].class) {
            typeName = "com.knockchat.hibernate.model.types.BinaryType";
        }else if (java.util.List.class.equals(javaType)){
            if (columnModel.getColumnType().contains("float")
                    ||columnModel.getColumnType().contains("float8")) {
                typeName = "com.knockchat.hibernate.model.types.DoubleListType";
            }else if (columnModel.getColumnType().contains("integer") ||
                    columnModel.getColumnType().contains("int") ){
                typeName = "com.knockchat.hibernate.model.types.IntegerListType";
            }else if (columnModel.getColumnType().contains("short")){
                typeName = "com.knockchat.hibernate.model.types.ShortListType";
            }else if (columnModel.getColumnType().contains("long")) {
                typeName = "com.knockchat.hibernate.model.types.LongListType";
            }else if (columnModel.getColumnType().contains("varchar") || columnModel.getColumnType().contains("text")) {
            typeName = "com.knockchat.hibernate.model.types.StringListType";
            }else {
                typeName = null;
            }
        }else if (Map.class.equals(javaType)){
        	if (columnModel.getColumnType().contains("hstore")){
        		typeName = "com.knockchat.hibernate.model.types.HstoreType";
                col.setCustomRead(col.getName()+"::hstore");
                col.setCustomWrite("?::hstore");
        	} else {
        		typeName = null;
        	}
        }else if (TEnum.class.isAssignableFrom(javaType)){
        	typeName = TEnumTypeFactory.create(javaType).getCanonicalName();
        	
        }else if (jdbcType == Types.DATE && com.knockchat.hibernate.model.types.DateType.isCompatible(javaType)) {
        	typeName = DateTypeFactory.create(javaType).getCanonicalName();
        	
        }else{
        	typeName = null;
        }

        if (typeName == null) {
            LOG.error("Unsupported type (java type: {}, jdbc type: {}) " +
                            "for column '{}'.",
                            javaType,
                            jdbcType,
                            columnModel );
            return false;
        }

        if (value != null) {
            value.setTypeName(typeName);
            if(typeParams != null) {
                value.setTypeParameters(typeParams);
            }
        }
        return true;
    }

    public void setMetaDataProvider(MetaDataProvider metaDataProvider) {
        this.metaDataProvider = metaDataProvider;
    }
}

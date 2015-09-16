package com.knockchat.hibernate.model;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.sql.DataSource;

import org.hibernate.annotations.OptimisticLocking;
import org.hibernate.annotations.SQLInsert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.DatabaseMetaDataCallback;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.MetaDataAccessException;

import com.google.common.base.CaseFormat;
import com.knockchat.utils.meta.MetaClass;
import com.knockchat.utils.meta.MetaClasses;
import com.knockchat.utils.meta.MetaProperty;

public class MetaDataProvider {

    public static final String TS_POSTFIX = "Ts";
    public static final String VIEW_POSTFIX = "_v";
    public static final String DEFAULT_PK_COLUMN = "id";
    private static final Logger LOG = LoggerFactory.getLogger(MetaDataProvider.class);

    private DataSource dataSource;
    private Properties tableNames;
    private List<Table> tableModels;

    public MetaDataProvider(DataSource dataSource, Properties tableNames) {
        this.dataSource = dataSource;
        this.tableNames = tableNames;
        tableModels = new ArrayList<>();
        readMetaData();
    }

    public List<Table> getTableModels() {
        return tableModels;
    }

    private void readMetaData() {
        for (final String schemaTable : tableNames.stringPropertyNames()) {
        	for (String m : tableNames.getProperty(schemaTable).split(","))
        		addTable(schemaTable, m);
        }
    }
    
    private void addTable(final String schemaTable, final String modelName){
        try {
            if (schemaTable.indexOf(".") < 0) {
                LOG.warn("Skip table {} due to can't find delimiter \".\" between schema.table", schemaTable);
                return;
            }
            
            final String schemaName = schemaTable.substring(0, schemaTable.indexOf("."));
            final String tableName = schemaTable.substring(schemaTable.indexOf(".") + 1);
            final Class clazz = Class.forName(modelName);
            
            final Table table = new Table(schemaName, tableName, clazz,tableName.endsWith(VIEW_POSTFIX));
            
            final List<Column> tableColumns = readColumns(table, clazz);
            final PrimaryKey pk = readPK(tableName);
            
            table.setColumns(tableColumns);
            table.setPrimaryKey(pk);
            
            
            if (clazz.isAnnotationPresent(OptimisticLocking.class)){
            	table.setOptimisticLockType(((OptimisticLocking)clazz.getAnnotation(OptimisticLocking.class)).type());
            }
            
            table.sqlInsert = (SQLInsert)clazz.getAnnotation(SQLInsert.class);
            
            tableModels.add(table);
        } catch (MetaDataAccessException e) {
            LOG.error("Error get model metadata {}", schemaTable, e);
        } catch (ClassNotFoundException e) {
            LOG.error("Can't find class {}", modelName, e);
        }   	
    }

    @SuppressWarnings({ "unchecked"})
	private List<Column> readColumns(final Table table, Class clazz) throws MetaDataAccessException {
        MetaClass meta = MetaClasses.get(clazz);
        List<Column> columns = (List<Column>) JdbcUtils.extractDatabaseMetaData(dataSource, new DatabaseMetaDataCallback() {
            @Override
            public Object processMetaData(DatabaseMetaData dbmd) throws SQLException, MetaDataAccessException {
                List<Column> columns = new Column.ColumnExtractor(table).extractData(dbmd.getColumns(null, null, table.getTableName().toLowerCase(), null));
                return columns;
            }
        });
        for (Column column : columns) {
            setColProperiesDetails(column , meta);
            column.setHibernateType();
        }
        return columns;
    }

    private void setColProperiesDetails(Column column, MetaClass meta){
        String lowerCase = column.getColumnName().toLowerCase();
        String camelCase = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, lowerCase.toUpperCase());
        String camelCaseTs = null;
        switch (column.getJdbcType()){
            case Types.TIMESTAMP:
            case Types.TIME:
            case Types.DATE: camelCaseTs = camelCase.concat(TS_POSTFIX);
        }
        
        final MetaProperty property = (camelCaseTs != null && meta.getProperty(camelCaseTs) != null) ? meta.getProperty(camelCaseTs)
                : meta.getProperty(camelCase) != null ? meta.getProperty(camelCase)
                : meta.getProperty(lowerCase);
                
        if (property != null) {
            column.setJavaClass(property.getType());
            column.setPropertyName(property.getName());
        }else{
        	//System.err.println("Coudn't find property " + column.getColumnName() + " in class " + meta.getName());
        }

    }

    private PrimaryKey readPK(final String tableName) throws MetaDataAccessException {
        PrimaryKey primaryKey = (PrimaryKey)JdbcUtils.extractDatabaseMetaData(dataSource, new DatabaseMetaDataCallback() {
            @Override
            public Object processMetaData(DatabaseMetaData dbmd) throws SQLException, MetaDataAccessException {
                PrimaryKey pk = new PrimaryKey.PKExtractor().extractData(dbmd.getPrimaryKeys(null, null, tableName.toLowerCase()));
                return pk;
            }
        });
        if (primaryKey == null && tableName.endsWith(VIEW_POSTFIX)){
            primaryKey = new PrimaryKey();
            primaryKey.setPrimaryKeyName(tableName+"_pk");
            primaryKey.addColumnName(DEFAULT_PK_COLUMN);
        }
        return primaryKey;
    }
    
    public String toHbmXml() {
    	final StringBuilder sb = new StringBuilder();
    	sb.append("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n");
    	//sb.append("<!DOCTYPE hibernate-mapping PUBLIC \n\"-//Hibernate/Hibernate Mapping DTD//EN\"\n \"http://www.hibernate.org/dtd/hibernate-mapping-3.0.dtd\">\n");
    	sb.append("<hibernate-mapping>\n");
    	for(Table t: tableModels){
    		
    		if (!t.isValid())
    			throw new RuntimeException("Table " + t.getTableName() + " is invalid");
    		
    		sb.append(t.toHbmXml().replaceAll("(?m)^", "\t"));
    	}
    	sb.append("</hibernate-mapping>");
    	sb.toString();
    	return sb.toString();
    }

}

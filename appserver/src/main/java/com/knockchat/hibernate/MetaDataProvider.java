package com.knockchat.hibernate;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.DatabaseMetaDataCallback;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.MetaDataAccessException;

import com.google.common.base.CaseFormat;
import com.knockchat.hibernate.model.Column;
import com.knockchat.hibernate.model.PrimaryKey;
import com.knockchat.hibernate.model.Table;
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
            try {
                if (schemaTable.indexOf(".") < 0) {
                    LOG.warn("Skip table {} due to can't find delimiter \".\" between schema.table", schemaTable);
                    continue;
                }
                String schemaName = schemaTable.substring(0, schemaTable.indexOf("."));
                String tableName = schemaTable.substring(schemaTable.indexOf(".") + 1);
                Class clazz = Class.forName(tableNames.getProperty(schemaTable));
                List<Column> tableColumns = readColumns(tableName, clazz);
                PrimaryKey pk = readPK(tableName);
                Table table = new Table(schemaName, tableName, clazz, tableColumns, pk, tableName.endsWith(VIEW_POSTFIX));
                tableModels.add(table);
            } catch (MetaDataAccessException e) {
                LOG.error("Error get model metadata {}", schemaTable, e);
            } catch (ClassNotFoundException e) {
                LOG.error("Can't find class {}", tableNames.getProperty(schemaTable.toString()), e);
            }
        }
    }

    private List<Column> readColumns(final String tableName, Class clazz) throws MetaDataAccessException {
        MetaClass meta = MetaClasses.get(clazz);
        List<Column> columns = (List<Column>) JdbcUtils.extractDatabaseMetaData(dataSource, new DatabaseMetaDataCallback() {
            @Override
            public Object processMetaData(DatabaseMetaData dbmd) throws SQLException, MetaDataAccessException {
                List<Column> columns = new Column.ColumnExtractor().extractData(dbmd.getColumns(null, null, tableName.toLowerCase(), null));
                return columns;
            }
        });
        for (Column column : columns) {
            setColProperiesDetails(column , meta);
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
        MetaProperty property = (camelCaseTs != null && meta.getProperty(camelCaseTs) != null) ? meta.getProperty(camelCaseTs)
                : meta.getProperty(camelCase) != null ? meta.getProperty(camelCase)
                : meta.getProperty(lowerCase);
        if (property != null) {
            column.setJavaClass(property.getType());
            column.setPropertyName(property.getName());
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

    private List<MetaProperty> readJavaFields(Class clazz){
        MetaClass metaClass = MetaClasses.get(clazz);
        return Arrays.asList(metaClass.getProperties());
    }

}

package org.everthrift.sql.hibernate.model;

import com.google.common.base.CaseFormat;
import org.apache.thrift.TDoc;
import org.hibernate.annotations.OptimisticLocking;
import org.hibernate.annotations.SQLInsert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.MetaDataAccessException;

import javax.sql.DataSource;
import java.beans.PropertyDescriptor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.springframework.beans.BeanUtils.getPropertyDescriptor;

public class MetaDataProvider {

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
            for (String m : tableNames.getProperty(schemaTable).split(",")) {
                addTable(schemaTable, m);
            }
        }
    }

    private void addTable(final String schemaTable, final String modelName) {

        LOG.info("Building HBM mapping: {} <-> {}", schemaTable, modelName);

        try {
            if (schemaTable.indexOf(".") < 0) {
                LOG.warn("Skip table {} due to can't find delimiter \".\" between schema.table", schemaTable);
                return;
            }

            final String schemaName = schemaTable.substring(0, schemaTable.indexOf("."));
            final String tableName = schemaTable.substring(schemaTable.indexOf(".") + 1);
            final Class clazz = Class.forName(modelName);

            final Table table = new Table(schemaName, tableName, clazz, tableName.endsWith(VIEW_POSTFIX));

            final List<Column> tableColumns = readColumns(table, clazz);
            final PrimaryKey pk = readPK(tableName);

            table.setColumns(tableColumns);
            table.setPrimaryKey(pk);

            if (clazz.isAnnotationPresent(OptimisticLocking.class)) {
                table.setOptimisticLockType(((OptimisticLocking) clazz.getAnnotation(OptimisticLocking.class)).type());
            }

            table.sqlInsert = (SQLInsert) clazz.getAnnotation(SQLInsert.class);

            tableModels.add(table);
        } catch (MetaDataAccessException e) {
            LOG.error("Error get model metadata {}", schemaTable, e);
        } catch (ClassNotFoundException e) {
            LOG.error("Can't find class {}", modelName, e);
        }
    }

    @SuppressWarnings({"unchecked"})
    private List<Column> readColumns(final Table table, Class clazz) throws MetaDataAccessException {
        List<Column> columns = (List<Column>) JdbcUtils.extractDatabaseMetaData(dataSource, dbmd -> {
            List<Column> columns1 = new Column.ColumnExtractor(table).extractData(dbmd.getColumns(null, null,
                                                                                                  table.getTableName()
                                                                                                       .toLowerCase(),
                                                                                                  null));
            return columns1;
        });
        for (Column column : columns) {
            setColProperiesDetails(column, clazz);
            column.setHibernateType();
        }
        return columns;
    }

    private void setColProperiesDetails(Column column, Class clazz) {
        final String lowerCase = column.getColumnName().toLowerCase();
        final String camelCase = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, lowerCase.toUpperCase());


        final PropertyDescriptor property = getPropertyDescriptor(clazz, camelCase) != null ?
                                            getPropertyDescriptor(clazz, camelCase) :
                                            getPropertyDescriptor(clazz, lowerCase);

        if (property != null) {

            final Type propertyType = property.getReadMethod().getGenericReturnType();
            if (propertyType instanceof ParameterizedType){
                column.setJavaClassParameters(((ParameterizedType)propertyType).getActualTypeArguments());
                column.setJavaClass((Class)((ParameterizedType)propertyType).getRawType());
            }else if (propertyType instanceof Class){
                column.setJavaClass((Class)propertyType);
            }else {
                throw new RuntimeException("Unknown type: " + propertyType.getClass().getCanonicalName());
            }

            column.setPropertyName(property.getName());

            if (property.getValue("doc") != null) {
                column.setDoc((String) property.getValue("doc"));
            } else if (property.getReadMethod().getAnnotation(TDoc.class) != null) {
                column.setDoc(property.getReadMethod().getAnnotation(TDoc.class).value());
            }
        } else {
            // System.err.println("Coudn't find property " +
            // column.getColumnName() + " in class " + meta.getName());
        }

    }

    private PrimaryKey readPK(final String tableName) throws MetaDataAccessException {
        PrimaryKey primaryKey = (PrimaryKey) JdbcUtils.extractDatabaseMetaData(dataSource,
                                                                               dbmd -> new PrimaryKey
                                                                                   .PKExtractor()
                                                                                   .extractData(dbmd.getPrimaryKeys(null, null, tableName
                                                                                       .toLowerCase())));

        if (primaryKey == null && tableName.endsWith(VIEW_POSTFIX)) {
            primaryKey = new PrimaryKey();
            primaryKey.setPrimaryKeyName(tableName + "_pk");
            primaryKey.addColumnName(DEFAULT_PK_COLUMN);
        }
        return primaryKey;
    }

    public String toHbmXml() {
        final StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n");
        // sb.append("<!DOCTYPE hibernate-mapping PUBLIC
        // \n\"-//Hibernate/Hibernate Mapping DTD//EN\"\n
        // \"http://www.hibernate.org/dtd/hibernate-mapping-3.0.dtd\">\n");
        sb.append("<hibernate-mapping>\n");
        for (Table t : tableModels) {

            if (!t.isValid()) {
                throw new RuntimeException("Table " + t.getTableName() + " is invalid");
            }

            sb.append(t.toHbmXml().replaceAll("(?m)^", "\t"));
        }
        sb.append("</hibernate-mapping>");
        sb.toString();
        return sb.toString();
    }

    public String getComments() {
        return tableModels.stream().flatMap(t -> t.getComments().stream()).collect(Collectors.joining("\n"));
    }

}

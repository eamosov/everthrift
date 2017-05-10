package org.everthrift.sql.hibernate.model;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import org.apache.thrift.TDoc;
import org.hibernate.annotations.OptimisticLockType;
import org.hibernate.annotations.ResultCheckStyle;
import org.hibernate.annotations.SQLInsert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Table {

    protected PrimaryKey primaryKey;

    protected Map<String, Column> columnsByName;

    protected String schema;

    protected String tableName;

    protected Class javaClass;

    protected boolean view = false;

    protected OptimisticLockType optimisticLockType = OptimisticLockType.NONE;

    protected SQLInsert sqlInsert;

    public Table(String schema, String tableName, Class<?> javaClass, boolean view) {
        this.view = view;
        this.schema = schema;
        this.tableName = tableName;
        this.javaClass = javaClass;
    }

    public void setColumns(List<Column> columns) {
        columnsByName = Maps.uniqueIndex(columns, new Function<Column, String>() {
            @Override
            public String apply(Column input) {
                return input.getColumnName();
            }
        });
    }

    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
    }

    public Map<String, Column> getColumnsByName() {
        return columnsByName;
    }

    public void addColumns(List<Column> columns) {
        for (Column col : columns) {
            columnsByName.put(col.getColumnName(), col);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchema() {
        return schema;
    }

    public Class getJavaClass() {
        return javaClass;
    }

    public boolean isView() {
        return view;
    }

    public boolean isValid() {
        return this.primaryKey != null && this.primaryKey.getColumnNames().size() > 0
            && this.columnsByName.keySet().containsAll(this.primaryKey.getColumnNames());
    }

    public OptimisticLockType getOptimisticLockType() {
        return optimisticLockType;
    }

    public void setOptimisticLockType(OptimisticLockType optimisticLockType) {
        this.optimisticLockType = optimisticLockType;
    }

    private String toXmlValue(ResultCheckStyle s) {
        switch (s) {
            case NONE:
                return "none";
            case COUNT:
                return "rowcount";
            case PARAM:
                return "param";
        }
        return null;
    }

    public String toHbmXml() {
        final StringBuilder sb = new StringBuilder();

        sb.append(String.format("<class name=\"%s\" table=\"%s\" schema=\"%s\" dynamic-update=\"true\" dynamic-insert=\"true\"  lazy=\"false\" optimistic-lock=\"%s\">\n",
                                javaClass.getCanonicalName(), tableName, schema, optimisticLockType.name()
                                                                                                   .toLowerCase()));

        if (primaryKey == null) {
            throw new RuntimeException("No PK for table " + this.tableName);
        }

        sb.append("\t<cache usage=\"nonstrict-read-write\" />\n");

        if (primaryKey.getColumnNames().size() == 1) {
            sb.append(getColumnsByName().get(primaryKey.getColumnNames().get(0))
                                        .toHbmXmlPk()
                                        .replaceAll("(?m)^", "\t"));
        } else {
            final StringBuilder ci = new StringBuilder();
            ci.append(String.format("<composite-id name=\"pk\" class=\"%s$Key\">\n", javaClass.getCanonicalName()));
            for (String pk : primaryKey.getColumnNames()) {
                ci.append(getColumnsByName().get(pk).toHbmXmlKeyProperty().replaceAll("(?m)^", "\t"));
                ci.append("\n");
            }
            ci.append("</composite-id>");
            sb.append(ci.toString().replaceAll("(?m)^", "\t"));
        }

        sb.append("\n");

        if (optimisticLockType == OptimisticLockType.VERSION) {
            final Column v = getColumnsByName().get("version");
            if (v == null) {
                throw new RuntimeException("No column \"version\" in type " + javaClass.getCanonicalName());
            }

            sb.append(v.toHbmXmlVersion().replaceAll("(?m)^", "\t"));
            sb.append("\n");
        }

        for (Column columnModel : getColumnsByName().values()) {

            if (!columnModel.isValid()) {
                continue;
            }

            final String col;
            if (primaryKey.getColumnNames().contains(columnModel.getColumnName())) {
                col = null;
            } else if (optimisticLockType == OptimisticLockType.VERSION && columnModel.getColumnName()
                                                                                      .equalsIgnoreCase("version")) {
                col = null;
            } else {
                col = columnModel.toHbmXml();
            }

            if (col != null) {
                sb.append(col.replaceAll("(?m)^", "\t"));
                sb.append("\n");
            }
        }

        if (sqlInsert != null) {
            sb.append(String.format("\t<sql-insert callable=\"%s\" check=\"%s\">%s</sql-insert>\n",
                                    Boolean.toString(sqlInsert.callable()),
                                    toXmlValue(sqlInsert.check()),
                                    sqlInsert.sql()));
        }

        sb.append("</class>\n");

        return sb.toString();
    }

    @Override
    public String toString() {
        return schema + "." + tableName;
    }

    public List<String> getComments() {
        final List<String> list = new ArrayList<>();
        Optional.ofNullable((TDoc) javaClass.getAnnotation(TDoc.class))
                .map(TDoc::value)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .ifPresent(c -> list.add(String.format("COMMENT ON TABLE %s.%s IS '%s';", schema, tableName, c.replace("'", "''"))));

        getColumnsByName().values().forEach(columnModel -> columnModel.getComment().ifPresent(list::add));
        return list;
    }
}

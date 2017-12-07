package org.everthrift.sql.hibernate.model;

import gnu.trove.map.hash.TLongLongHashMap;
import org.apache.thrift.TEnum;
import org.everthrift.sql.hibernate.ThriftPropertyAccessStrategy;
import org.everthrift.sql.hibernate.model.types.BoxType;
import org.everthrift.sql.hibernate.model.types.CustomTypeFactory;
import org.everthrift.sql.hibernate.model.types.DoubleListType;
import org.everthrift.sql.hibernate.model.types.IntegerListType;
import org.everthrift.sql.hibernate.model.types.JsonType;
import org.everthrift.sql.hibernate.model.types.LongDateType;
import org.everthrift.sql.hibernate.model.types.LongIntervalType;
import org.everthrift.sql.hibernate.model.types.LongListType;
import org.everthrift.sql.hibernate.model.types.LongTimestampType;
import org.everthrift.sql.hibernate.model.types.PointType;
import org.everthrift.sql.hibernate.model.types.ShortListType;
import org.everthrift.sql.hibernate.model.types.StringListType;
import org.everthrift.sql.hibernate.model.types.StringSetType;
import org.everthrift.sql.hibernate.model.types.StringUUIDType;
import org.everthrift.sql.hibernate.model.types.TBaseType;
import org.everthrift.sql.hibernate.model.types.TEnumListTypeFactory;
import org.everthrift.sql.hibernate.model.types.TEnumTypeFactory;
import org.everthrift.sql.hibernate.model.types.TLongLongHstoreType;
import org.everthrift.sql.hibernate.model.types.UUIDStringListType;
import org.everthrift.sql.hibernate.model.types.UUIDStringSetType;
import org.everthrift.thrift.TBaseHasModel;
import org.everthrift.thrift.TBaseModel;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.util.StringUtils;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Column {

    private static final Logger log = LoggerFactory.getLogger(Column.class);

    protected final Table table;

    protected String columnName;

    protected int jdbcType;

    protected String columnType;

    protected boolean nullable;

    protected Integer length;

    protected Integer scale;

    protected Class javaClass;

    protected Type javaClassParameters[];

    protected String propertyName;

    protected boolean isAutoincrement;

    @Nullable
    protected String hibernateType;


    //TODO нужно убрать customRead/Write, т.к. в случае NativeQuery преобразования не происходит и ResultSet некорректно преобразуется в объект
    protected String customRead;

    protected String customWrite;

    protected String doc;

    public Column(Table table) {
        this.table = table;
    }

    public String getDoc() {
        return doc;
    }

    public void setDoc(String doc) {
        this.doc = doc;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public void setJdbcType(int jdbcType) {
        this.jdbcType = jdbcType;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public Class getJavaClass() {
        return javaClass;
    }

    public void setJavaClass(Class javaClass) {
        this.javaClass = javaClass;
    }

    public Type[] getJavaClassParameters() {
        return javaClassParameters;
    }

    public void setJavaClassParameters(Type javaClassParameters[]) {
        this.javaClassParameters = javaClassParameters;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    public boolean isValid() {
        return this.propertyName != null && this.javaClass != null && this.hibernateType != null;
    }

    public static class ColumnExtractor implements ResultSetExtractor<List<Column>> {

        private final Table table;

        public ColumnExtractor(Table table) {
            super();
            this.table = table;
        }

        @NotNull
        @Override
        public List<Column> extractData(@NotNull ResultSet rs) throws SQLException, DataAccessException {
            List<Column> columns = new ArrayList<>();
            while (rs.next()) {
                Column column = new Column(table);
                column.setColumnName(rs.getString("COLUMN_NAME"));
                column.setJdbcType(rs.getInt("DATA_TYPE"));
                column.setLength(rs.getInt("COLUMN_SIZE"));
                column.setScale(rs.getInt("DECIMAL_DIGITS"));
                column.setNullable(rs.getInt("NULLABLE") > 0);
                column.setColumnType(rs.getString("TYPE_NAME"));
                column.setAutoincrement(rs.getString("IS_AUTOINCREMENT").equals("YES"));

                if (column.isAutoincrement() == false && column.getColumnName().equalsIgnoreCase("id")
                    && !StringUtils.isEmpty(rs.getString("COLUMN_DEF")) && !column.isNullable()) {

                    column.setAutoincrement(true);
                }

                columns.add(column);
            }
            return columns;
        }
    }

    public boolean isAutoincrement() {
        return isAutoincrement;
    }

    public void setAutoincrement(boolean isAutoincrement) {
        this.isAutoincrement = isAutoincrement;
    }

    @NotNull
    @Override
    public String toString() {
        return "Column [columnName=" + columnName + ", jdbcType=" + jdbcType + ", columnType=" + columnType + ", nullable=" + nullable
            + ", length=" + length + ", scale=" + scale + ", javaClass=" + javaClass + ", propertyName=" + propertyName
            + ", isAutoincrement=" + isAutoincrement + "]";
    }

    public boolean setHibernateType() {

        final String logFmt = "{}.{}({}/{})  <-> {}.{}({})";
        final Object[] logArgs = new Object[]{table.getTableName(), columnName, jdbcType, columnType, table.javaClass.getSimpleName(),
            propertyName, javaClass != null ? javaClass.getSimpleName() : "null"};

        if (propertyName == null) {
            log.trace("Skip not existing " + logFmt, logArgs);
            return false;
        } else if (javaClass == null) {
            log.warn(logFmt, logArgs);
            return false;
        } else {
            log.debug(logFmt, logArgs);
        }

        if (javaClass == Long.class || javaClass == long.class) {
            switch (jdbcType) {
                case Types.TIMESTAMP:
                    hibernateType = LongTimestampType.class.getCanonicalName();
                    break;

                case Types.DATE:
                    hibernateType = LongDateType.class.getCanonicalName();
                    break;

                case Types.OTHER:
                    if (columnType.equalsIgnoreCase("interval")) {
                        hibernateType = LongIntervalType.class.getCanonicalName();
                        customRead = "extract(epoch from " + columnName + ")";
                    }
                    break;
                default:
                    hibernateType = LongType.INSTANCE.getName();
            }
        } else if (javaClass == Short.class || javaClass == short.class) {

            hibernateType = ShortType.INSTANCE.getName();

        } else if (javaClass == Integer.class || javaClass == int.class) {

            hibernateType = IntegerType.INSTANCE.getName();

        } else if (javaClass == Byte.class || javaClass == byte.class) {

            hibernateType = ByteType.INSTANCE.getName();

        } else if (javaClass == Float.class || javaClass == float.class) {

            hibernateType = FloatType.INSTANCE.getName();

        } else if (javaClass == Double.class || javaClass == double.class) {

            hibernateType = DoubleType.INSTANCE.getName();

        } else if (javaClass == Character.class || javaClass == char.class) {

            hibernateType = CharacterType.INSTANCE.getName();

        } else if (javaClass == String.class) {

            hibernateType = StringType.INSTANCE.getName();

            if (jdbcType == Types.TIMESTAMP) {

                customRead = columnName + "::text";
                customWrite = "?::timestamp";

            } else if (columnType.equals("geography")) {

                customRead = columnName + "::text";
                customWrite = "?::geography";

            } else if (columnType.equals("inet")) {

                customRead = columnName + "::text";
                customWrite = "?::inet";

            } else if (columnType.equals("uuid")) {
                hibernateType = StringUUIDType.class.getCanonicalName();
            }

        } else if (java.util.Date.class.isAssignableFrom(javaClass)) {

            switch (jdbcType) {
                case Types.DATE:
                    hibernateType = DateType.INSTANCE.getName();
                    break;
                case Types.TIME:
                    hibernateType = TimeType.INSTANCE.getName();
                    break;
                case Types.TIMESTAMP:
                    hibernateType = TimestampType.INSTANCE.getName();
                    break;
            }

        } else if (javaClass == Boolean.class || javaClass == boolean.class) {

            if (jdbcType == Types.BIT || jdbcType == Types.BOOLEAN) {
                hibernateType = BooleanType.INSTANCE.getName();
            } else if (jdbcType == Types.NUMERIC || jdbcType == Types.DECIMAL || jdbcType == Types.INTEGER || jdbcType == Types.SMALLINT
                || jdbcType == Types.TINYINT || jdbcType == Types.BIGINT) {
                hibernateType = NumericBooleanType.INSTANCE.getName();
            }

        } else if (javaClass == BigDecimal.class) {

            hibernateType = BigDecimalType.INSTANCE.getName();

        } else if (javaClass == BigInteger.class) {

            hibernateType = BigIntegerType.INSTANCE.getName();

        } else if (javaClass == byte[].class) {

            hibernateType = org.everthrift.sql.hibernate.model.types.BinaryType.class.getCanonicalName();

        } else if (java.util.List.class.equals(javaClass)) {

            if (columnType.contains("float") || columnType.contains("float8")) {

                hibernateType = DoubleListType.class.getCanonicalName();

            } else if (columnType.contains("_int8")) {

                hibernateType = LongListType.class.getCanonicalName();

            } else if (columnType.contains("_int4")) {

                if (Integer.class.isAssignableFrom((Class) javaClassParameters[0])) {
                    hibernateType = IntegerListType.class.getCanonicalName();
                } else if (TEnum.class.isAssignableFrom((Class) javaClassParameters[0])) {
                    hibernateType = TEnumListTypeFactory.create((Class) javaClassParameters[0]).getCanonicalName();
                } else {
                    throw new RuntimeException("Don't know how to map list<" + javaClassParameters[0].getTypeName() + "> to _int4");
                }

            } else if (columnType.contains("_short")) {

                hibernateType = ShortListType.class.getCanonicalName();

            } else if (columnType.contains("_varchar") || columnType.contains("_text")) {

                hibernateType = StringListType.class.getCanonicalName();
            } else if (columnType.equals("_uuid")) {
                hibernateType = UUIDStringListType.class.getCanonicalName();
            }

        } else if (java.util.Set.class.equals(javaClass)) {

            if (columnType.contains("_varchar") || columnType.contains("_text")) {
                hibernateType = StringSetType.class.getCanonicalName();
            } else if (columnType.equals("_uuid")) {
                hibernateType = UUIDStringSetType.class.getCanonicalName();
            }

        } else if (Map.class.equals(javaClass)) {

            if (columnType.contains("hstore")) {
                hibernateType = org.everthrift.sql.hibernate.model.types.HstoreType.class.getCanonicalName();
                customRead = columnName + "::hstore";
                customWrite = "?::hstore";
            }

        } else if (TLongLongHashMap.class.equals(javaClass)) {

            if (columnType.contains("hstore")) {
                hibernateType = TLongLongHstoreType.class.getCanonicalName();
                customRead = columnName + "::hstore";
                customWrite = "?::hstore";
            }

        } else if (TEnum.class.isAssignableFrom(javaClass)) {

            hibernateType = TEnumTypeFactory.create(javaClass).getCanonicalName();

        } else if (jdbcType == Types.DATE && org.everthrift.sql.hibernate.model.types.DateType.isCompatible(javaClass)) {

            hibernateType = CustomTypeFactory.create(javaClass, org.everthrift.sql.hibernate.model.types.DateType.class)
                                             .getCanonicalName();

        } else if (jdbcType == Types.OTHER && columnType.contains("box2d") && BoxType.isCompatible(javaClass)) {

            hibernateType = CustomTypeFactory.create(javaClass, BoxType.class).getCanonicalName();

        } else if (jdbcType == Types.OTHER && columnType.contains("geometry") && PointType.isCompatible(javaClass)) {

            hibernateType = CustomTypeFactory.create(javaClass, PointType.class).getCanonicalName();
            customRead = "st_astext(" + columnName + ")";
            customWrite = "?::geometry";

        } else if (jdbcType == Types.OTHER && columnType.contains("jsonb")) {

            final Class model = TBaseHasModel.getModel(javaClass);
            hibernateType = CustomTypeFactory.create(model != null ? model : javaClass, JsonType.class)
                                             .getCanonicalName();
        } else if (jdbcType == Types.BINARY && TBaseModel.class.isAssignableFrom(javaClass)) {

            hibernateType = CustomTypeFactory.create(javaClass, TBaseType.class).getCanonicalName();
        } else if (jdbcType == Types.BINARY && TBaseHasModel.getModel(javaClass) != null) {

            hibernateType = CustomTypeFactory.create(TBaseHasModel.getModel(javaClass), TBaseType.class)
                                             .getCanonicalName();
        }

        if (hibernateType == null) {
            hibernateType = CustomTypesRegistry.getInstance()
                                               .get(table.javaClass, javaClass, propertyName, jdbcType, columnType, columnName);
        }

        if (hibernateType == null) {
            log.error("Unknown mapping " + logFmt, logArgs);
            throw new RuntimeException("Coudn't map some fields");
        }

        return true;
    }

    @Nullable
    public String toHbmXmlVersion() {
        if (!this.isValid()) {
            return null;
        }

        final StringBuilder sb = new StringBuilder();

        sb.append(String.format("<version name=\"%s\" column=\"%s\" type=\"%s\" access=\"%s\"/>\n", propertyName, columnName, hibernateType,
                                ThriftPropertyAccessStrategy.class.getCanonicalName()));

        return sb.toString();
    }

    @Nullable
    public String toHbmXmlKeyProperty() {
        if (!this.isValid()) {
            return null;
        }
        return String.format("<key-property name=\"%s\" column=\"%s\" type=\"%s\"/>", propertyName, columnName, hibernateType);
    }

    @Nullable
    public String toHbmXmlPk() {
        if (!this.isValid()) {
            return null;
        }

        final StringBuilder sb = new StringBuilder();

        sb.append(String.format("<id name=\"%s\" type=\"%s\" access=\"%s\">", propertyName, hibernateType,
                                ThriftPropertyAccessStrategy.class.getCanonicalName()));

        String column = String.format("<column name=\"%s\" not-null=\"true\" sql-type=\"%s\" ", columnName, columnType);

        if (customRead != null) {
            column += String.format("read=\"%s\" ", customRead);
        }

        if (customWrite != null) {
            column += String.format("write=\"%s\" ", customWrite);
        }

        column += "/>";

        final String generator = String.format("<generator class=\"%s\"/>",
                                               (isAutoincrement() || table.isView()) ? "identity" : "assigned");

        sb.append("\n\t");
        sb.append(column);
        sb.append("\n\t");
        sb.append(generator);
        sb.append("\n</id>\n");

        return sb.toString();
    }

    @Nullable
    public String toHbmXml() {
        if (!this.isValid()) {
            return null;
        }

        final StringBuilder sb = new StringBuilder();

        sb.append(String.format("<property name=\"%s\" not-null=\"%s\" type=\"%s\" lazy=\"false\" optimistic-lock=\"true\" update=\"true\" insert=\"true\" access=\"%s\">",
                                propertyName, Boolean.toString(!nullable), hibernateType,
                                ThriftPropertyAccessStrategy.class.getCanonicalName()));

        String column = String.format("<column name=\"%s\" not-null=\"%s\" sql-type=\"%s\" ", columnName, Boolean.toString(!nullable),
                                      columnType);

        if (customRead != null) {
            column += String.format("read=\"%s\" ", customRead);
        }

        if (customWrite != null) {
            column += String.format("write=\"%s\" ", customWrite);
        }

        column += "/>";

        sb.append("\n\t");
        sb.append(column);
        sb.append("\n</property>\n");

        return sb.toString();
    }

    public Optional<String> getComment() {
        return Optional.ofNullable(doc)
                       .map(String::trim)
                       .filter(s -> !s.isEmpty())
                       .map(s -> String.format("COMMENT ON COLUMN %s.%s.%s IS '%s';", table.getSchema(), table.getTableName(), columnName, s
                           .replace("'", "''")));
    }
}

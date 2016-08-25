package org.everthrift.sql.hibernate.model;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import com.google.common.collect.Lists;

public class PrimaryKey {

    protected String primaryKeyName;

    protected List<String> columnNames = Lists.newArrayList();

    protected short keySequence;

    public String getPrimaryKeyName() {
        return primaryKeyName;
    }

    public void setPrimaryKeyName(String primaryKeyName) {
        this.primaryKeyName = primaryKeyName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void addColumnName(String columnName) {
        this.columnNames.add(columnName);
    }

    public static class PKExtractor implements ResultSetExtractor<PrimaryKey> {

        @Override
        public PrimaryKey extractData(ResultSet rs) throws SQLException, DataAccessException {
            PrimaryKey pk = new PrimaryKey();
            while (rs.next()) {
                pk.setPrimaryKeyName(rs.getString("PK_NAME"));
                pk.addColumnName(rs.getString("COLUMN_NAME"));
                // pk.setKeySequence(rs.getShort("KEY_SEQ"));
            }
            return pk.getColumnNames().isEmpty() ? null : pk;
        }
    }
}

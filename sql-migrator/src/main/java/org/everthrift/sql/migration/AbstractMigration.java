package org.everthrift.sql.migration;

import org.apache.commons.io.IOUtils;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.sql.Types;

public abstract class AbstractMigration {

    private JdbcTemplate jdbcTemplate;

    public abstract void up();

    public abstract void down();

    public void createRowInMigrationTbl() {
        Migration m = getClass().getAnnotation(Migration.class);
        if (m != null) {
            getJdbcTemplate().update("insert into yii_migration(version, apply_time, module) values(?, ?, ?)",
                                     new Object[]{m.version(), System.currentTimeMillis() / 1000, m.module()},
                                     new int[]{Types.VARCHAR, Types.INTEGER, Types.VARCHAR});
        }
    }

    public void deleteRowInMigrationTbl() {
        Migration m = getClass().getAnnotation(Migration.class);
        if (m != null) {
            getJdbcTemplate().update("delete from yii_migration where version=? and  module=? ", new Object[]{m.version(), m.module()},
                                     new int[]{Types.VARCHAR, Types.VARCHAR});
        }
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public void execute(String sql) {
        this.getJdbcTemplate().execute(sql);
    }

    public void execute(Resource sql) {
        try {
            execute(IOUtils.toString(sql.getInputStream()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}

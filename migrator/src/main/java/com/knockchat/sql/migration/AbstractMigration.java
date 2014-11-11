package com.knockchat.sql.migration;

import java.sql.Types;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

public abstract class AbstractMigration {

    private JdbcTemplate jdbcTemplate;

    public abstract void up();
    public abstract void down();

    public void createRowInMigrationTbl(){
        Migration m = getClass().getAnnotation(Migration.class);
        if (m!=null) {
            getJdbcTemplate().update("insert into yii_migration(version, apply_time, module) values(?, ?, ?)", new Object[]{m.version(), System.currentTimeMillis()/1000 , m.module()}, new int[]{Types.VARCHAR,Types.INTEGER,Types.VARCHAR} );
        }
    }

    public void deleteRowInMigrationTbl(){
        Migration m = getClass().getAnnotation(Migration.class);
        if (m!=null) {
            getJdbcTemplate().update("delete from yii_migration where version=? and  module=? ", new Object[]{m.version(), m.module()}, new int[]{Types.VARCHAR,Types.VARCHAR} );
        }
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }
    
    public void execute(String sql){
    	this.getJdbcTemplate().execute(sql);
    }

}

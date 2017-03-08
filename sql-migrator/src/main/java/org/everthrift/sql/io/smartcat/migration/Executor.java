package org.everthrift.sql.io.smartcat.migration;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Executor is a class which executes all the migration for given session.
 */
public class Executor {

    private Executor() {
    }

    /**
     * Execute all migrations in migration resource collection.
     *
     * @param jdbcTemplate Datastax driver sesison object
     * @param resources    Migration resources collection
     * @return Return success
     */
    public static boolean migrate(final JdbcTemplate jdbcTemplate, final MigrationResources resources, final String schemaVersionCf) {
        return MigrationEngine.withJdbcTemplate(jdbcTemplate, schemaVersionCf).migrate(resources);
    }

}

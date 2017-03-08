package org.everthrift.sql.io.smartcat.migration;

import org.everthrift.sql.io.smartcat.migration.exceptions.MigrationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Migration engine wraps Migrator and provides DSL like API.
 */
public class MigrationEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationEngine.class);

    private MigrationEngine() {
    }

    /**
     * Create migrator out of session fully prepared for doing migration of
     * resources.
     *
     * @param jdbcTemplate Datastax driver session object
     * @return migrator instance with versioner and session which can migrate
     * resources
     */
    public static Migrator withJdbcTemplate(final JdbcTemplate jdbcTemplate, final String schemaVersionCf) {
        return new Migrator(jdbcTemplate, schemaVersionCf);
    }

    public static Migrator withJdbcTemplate(final JdbcTemplate jdbcTemplate) {
        return new Migrator(jdbcTemplate, "schema_version");
    }

    /**
     * Migrator handles migrations and errors.
     */
    public static class Migrator {
        private final JdbcTemplate jdbcTemplate;

        private final SqlVersioner versioner;

        /**
         * Create new Migrator with active Cassandra session.
         *
         * @param jdbcTemplate Active Cassandra session
         */
        Migrator(final JdbcTemplate jdbcTemplate, final String schemaVersionCf) {
            this.jdbcTemplate = jdbcTemplate;
            this.versioner = new SqlVersioner(jdbcTemplate, schemaVersionCf);
        }

        public boolean migrate(final MigrationResources resources) {
            return migrate(resources, false);
        }

        /**
         * Method that executes all migration from migration resources that are
         * higher version than db version. If migration fails, method will exit.
         *
         * @param resources Collection of migrations to be executed
         * @return Success of migration
         */
        public boolean migrate(final MigrationResources resources, boolean force) {
            LOGGER.debug("Start migration");

            for (final Migration migration : resources.getMigrations()) {
                final MigrationType type = migration.getType();
                final int migrationVersion = migration.getVersion();

                if (!force) {
                    final int version = versioner.getCurrentVersion(type);

                    LOGGER.info("Db is version {} for type {}.", version, type.name());
                    LOGGER.info("Compare {} migration version {} with description {}", type.name(), migrationVersion,
                                migration.getDescription());

                    if (migrationVersion <= version) {
                        LOGGER.warn("Skipping migration [{}] with version {} since db is on higher version {}.", migration
                                        .getDescription(),
                                    migrationVersion, version);
                        continue;
                    }

                    LOGGER.debug("Checking version second time with ALL consistency");
                    final int allVersion = versioner.getCurrentVersion(type);
                    if (allVersion != version) {
                        LOGGER.error("Version mismatch error, allVersion={}, version={}, migrationVersion={}", allVersion, version, migrationVersion);
                        return false;
                    }
                } else {
                    //Check ConsistencyLevel.ALL is available
                    versioner.getCurrentVersion(type);
                }

                migration.setJdbcTemplate(jdbcTemplate);

                final long start = System.currentTimeMillis();
                LOGGER.info("Start executing migration to version {}.", migrationVersion);

                try {
                    migration.execute();
                } catch (final MigrationException e) {
                    LOGGER.error("Failed to execute migration version {}, exception {}!", migrationVersion, e.getMessage());
                    LOGGER.debug("Exception stack trace: {}", e);
                    return false;
                }

                final long end = System.currentTimeMillis();
                final long seconds = (end - start) / 1000;
                LOGGER.info("Migration [{}] to version {} finished in {} seconds.", migration.getDescription(), migrationVersion, seconds);

                versioner.updateVersion(migration);
            }

            return true;
        }
    }

}

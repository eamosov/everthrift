package org.everthrift.sql.io.smartcat.migration;

import org.everthrift.sql.io.smartcat.migration.exceptions.MigrationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Migrator handles migrations and errors.
 */
public class MigrationEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationEngine.class);

    private final PlatformTransactionManager txManager;

    private final JdbcTemplate jdbcTemplate;

    private final SqlVersioner versioner;

    /**
     * Create new Migrator with active Cassandra session.
     *
     * @param jdbcTemplate Active Cassandra session
     */
    public MigrationEngine(PlatformTransactionManager txManager, final JdbcTemplate jdbcTemplate, final String schemaVersionCf) {
        this.txManager = txManager;
        this.jdbcTemplate = jdbcTemplate;
        this.versioner = new SqlVersioner(jdbcTemplate, schemaVersionCf);
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

        final TransactionTemplate txTemplate = new TransactionTemplate(txManager);


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

            txTemplate.execute(status -> {
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
                return  true;
            });

        }

        return true;
    }
}


package org.everthrift.sql.io.smartcat.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;

/**
 * Class responsible for version management.
 */
public class SqlVersioner {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlVersioner.class);

    private final String SCHEMA_VERSION_CF;

    private static final String TYPE = "type";

    private static final String VERSION = "version";

    private static final String TIMESTAMP = "ts";

    private static final String DESCRIPTION = "description";

    private final String CREATE_SCHEMA_VERSION_SQL;

    private final JdbcTemplate jdbcTemplate;

    /**
     * Create Cassandra versioner for active session.
     *
     * @param jdbcTemplate
     */
    public SqlVersioner(JdbcTemplate jdbcTemplate, final String schemaVersionCf) {
        this.jdbcTemplate = jdbcTemplate;
        this.SCHEMA_VERSION_CF = schemaVersionCf;

        CREATE_SCHEMA_VERSION_SQL = String.format("CREATE TABLE IF NOT EXISTS %s (", SCHEMA_VERSION_CF) + String.format("%s text,", TYPE)
            + String.format("%s int,", VERSION) + String.format("%s timestamp with time zone,", TIMESTAMP)
            + String.format("%s text,", DESCRIPTION) + String.format("PRIMARY KEY (%s, %s)", TYPE, VERSION)
            + String.format(")");

        createSchemaVersion();
    }

    private void createSchemaVersion() {
        LOGGER.debug("Try to create schema version column family");
        this.jdbcTemplate.execute(CREATE_SCHEMA_VERSION_SQL);
    }

    /**
     * Get current database version for given migration type with ALL
     * consistency. Select one row since migration history is saved ordered
     * descending by timestamp. If there are no rows in the schema_version
     * table, return 0 as default database version. Data version is changed by
     * executing migrations.
     *
     * @param type Migration type
     * @return Database version for given type
     */
    public int getCurrentVersion(final MigrationType type) {

        return jdbcTemplate.query(String.format("SELECT * FROM %s WHERE %s='%s' ORDER BY %s DESC LIMIT 1", SCHEMA_VERSION_CF, TYPE, type
                                      .name(), VERSION),
                                  rs -> rs.next() ? rs.getInt(VERSION) : 0);
    }

    /**
     * Update current database version to the migration version. This is
     * executed after migration success.
     *
     * @param migration Migration that updated the database version
     * @return Success of version update
     */
    public void updateVersion(final Migration migration) {


        jdbcTemplate.update(String.format("INSERT INTO %s(%s,%s,%s,%s) VALUES (?,?,?,?)",
                                          SCHEMA_VERSION_CF,
                                          TYPE,
                                          VERSION,
                                          TIMESTAMP,
                                          DESCRIPTION),
                            migration.getType().name(),
                            migration.getVersion(),
                            new Timestamp(System.currentTimeMillis()),
                            migration.getDescription());

    }
}

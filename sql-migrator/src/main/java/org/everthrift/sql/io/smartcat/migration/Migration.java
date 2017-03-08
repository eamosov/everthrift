package org.everthrift.sql.io.smartcat.migration;

import org.everthrift.sql.io.smartcat.migration.exceptions.MigrationException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Abstract migration class that implements session DI and exposes required
 * methods for execution.
 */
public abstract class Migration {

    private static final Pattern versionPattern = Pattern.compile("[^0-9]*([0-9]+).*");

    private MigrationType type = MigrationType.SCHEMA;

    /**
     * Active Cassandra session.
     */
    protected JdbcTemplate jdbcTemplate;

    /**
     * Create new migration with provided type and version.
     *
     * @param type Migration type (SCHEMA or DATA)
     */
    public Migration(final MigrationType type) {
        this.type = type;
    }

    /**
     * Enables session injection into migration class.
     *
     * @param jdbcTemplate Session object
     */
    public void setJdbcTemplate(final JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Returns migration type (schema or data).
     *
     * @return Migration type
     */
    public MigrationType getType() {
        return this.type;
    }

    protected int extractVersion(String name) {
        final Matcher m = versionPattern.matcher(name);
        if (m.matches()) {
            return Integer.parseInt(m.group(1), 10);
        }

        throw new RuntimeException("Coudn't parse migration version");
    }

    /**
     * Returns resulting database schema version of this migration.
     *
     * @return Resulting db schema version
     */
    public int getVersion() {
        return extractVersion(this.getClass().getSimpleName());
    }

    /**
     * Returns migration description (for history purposes).
     *
     * @return migration description.
     */
    public abstract String getDescription();

    /**
     * Executes migration implementation.
     *
     * @throws MigrationException exception
     */
    public abstract void execute() throws MigrationException;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + getVersion();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Migration other = (Migration) obj;
        if (type != other.type) {
            return false;
        }
        if (getVersion() != other.getVersion()) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Migration [type=" + type + ", version=" + getVersion() + "]";
    }
}

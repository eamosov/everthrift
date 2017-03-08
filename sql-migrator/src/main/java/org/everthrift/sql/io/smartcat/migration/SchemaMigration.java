package org.everthrift.sql.io.smartcat.migration;

/**
 * Schema migration for migrations manipulating schema.
 */
public abstract class SchemaMigration extends Migration {

    /**
     * Create new schema migration with provided version.
     */
    public SchemaMigration() {
        super(MigrationType.SCHEMA);
    }
}

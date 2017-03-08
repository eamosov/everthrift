package org.everthrift.sql.io.smartcat.migration;

/**
 * Data migration for migrations manipulating data.
 */
public abstract class DataMigration extends Migration {

    /**
     * Creates new data migration.
     **/
    public DataMigration() {
        super(MigrationType.DATA);
    }
}

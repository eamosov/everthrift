package io.smartcat.migration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Class which holds set of migrations.
 */
public class MigrationResources {

    private final List<Migration> migrations = new ArrayList<>();

    /**
     * Add Migration object to migration collection.
     * @param migration Migration object
     */
    public void addMigration(final Migration migration) {
        this.migrations.add(migration);
    }

    /**
     * Add Migration object collection to migration collection (set is used as
     * internal collection so no duplicates will be added and order will be
     * preserved meaning that if migration was in collection on position it will
     * stay on that position).
     * @param migrations Migration object list
     */
    public void addMigrations(final Collection<Migration> migrations) {
        this.migrations.addAll(migrations);
    }

    /**
     * Get all Migration objects sorted by order of insert.
     * @return Sorted list of Migration objects
     */
    public List<Migration> getMigrations() {
        return this.migrations;
    }
}

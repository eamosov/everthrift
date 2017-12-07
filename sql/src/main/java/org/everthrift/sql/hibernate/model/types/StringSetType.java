package org.everthrift.sql.hibernate.model.types;

import org.jetbrains.annotations.NotNull;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

public class StringSetType extends HibernateSetType<String> {

    @Override
    public Array createArray(@NotNull Set<String> object, @NotNull Connection connection) throws SQLException {
        Array array = connection.createArrayOf("varchar", object.toArray());
        return array;
    }
}

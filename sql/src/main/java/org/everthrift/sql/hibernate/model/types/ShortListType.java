package org.everthrift.sql.hibernate.model.types;

import org.jetbrains.annotations.NotNull;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class ShortListType extends HibernateListType<Short> {

    @Override
    public Array createArray(@NotNull List<Short> object, @NotNull Connection connection) throws SQLException {
        Array array = connection.createArrayOf("short", object.toArray());
        return array;
    }
}

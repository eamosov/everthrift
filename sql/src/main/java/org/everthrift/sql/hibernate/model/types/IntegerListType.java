package org.everthrift.sql.hibernate.model.types;

import org.jetbrains.annotations.NotNull;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class IntegerListType extends HibernateListType<Integer> {

    @Override
    public Array createArray(@NotNull List<Integer> object, @NotNull Connection connection) throws SQLException {
        Array array = connection.createArrayOf("integer", object.toArray());
        return array;
    }
}

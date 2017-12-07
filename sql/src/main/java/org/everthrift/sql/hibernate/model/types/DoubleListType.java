package org.everthrift.sql.hibernate.model.types;

import org.jetbrains.annotations.NotNull;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class DoubleListType extends HibernateListType<List<Double>> {

    @Override
    public Array createArray(@NotNull List object, @NotNull Connection connection) throws SQLException {
        Array array = connection.createArrayOf("float8", object.toArray());
        return array;
    }
}

package com.knockchat.hibernate.model.types;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class ShortListType extends HibernateListType<Short> {

    @Override
    public Array createArray(List<Short> object, Connection connection) throws SQLException {
        Array array = connection.createArrayOf("short", object.toArray());
        return array;
    }
}

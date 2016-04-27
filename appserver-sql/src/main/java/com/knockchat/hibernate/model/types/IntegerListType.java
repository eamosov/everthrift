package com.knockchat.hibernate.model.types;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class IntegerListType extends HibernateListType<Integer> {

    @Override
    public Array createArray(List<Integer> object, Connection connection) throws SQLException {
        Array array = connection.createArrayOf("integer", object.toArray());
        return array;
    }
}

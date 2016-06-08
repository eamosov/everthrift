package com.knockchat.sql.hibernate.model.types;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

public class StringSetType extends HibernateSetType<String> {

    @Override
    public Array createArray(Set<String> object, Connection connection) throws SQLException {
        Array array = connection.createArrayOf("varchar", object.toArray());
        return array;
    }
}

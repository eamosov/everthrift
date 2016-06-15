package org.everthrift.sql.hibernate.model.types;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class StringListType extends HibernateListType<String> {

    @Override
    public Array createArray(List<String> object, Connection connection) throws SQLException {
        Array array = connection.createArrayOf("varchar", object.toArray());
        return array;
    }
}

package org.everthrift.sql.hibernate.model.types;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class LongListType extends HibernateListType<Long> {

    @Override
    public Array createArray(List<Long> object, Connection connection) throws SQLException {
        Array array = connection.createArrayOf("bigint", object.toArray());
        return array;
    }
}

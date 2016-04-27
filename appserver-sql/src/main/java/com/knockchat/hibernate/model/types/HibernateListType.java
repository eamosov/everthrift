package com.knockchat.hibernate.model.types;

import java.io.Serializable;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.usertype.UserType;

import com.google.common.collect.Lists;


public abstract class HibernateListType<T> implements UserType {

    @Override
    public Class<List> returnedClass() {
        return List.class;
    }

    @Override
    public void nullSafeSet(final PreparedStatement statement, final Object object, final int i, final SessionImplementor sessionImplementor) throws HibernateException, SQLException {
        statement.setArray(i, object == null ? null : createArray((List<T>) object, statement.getConnection()));
    }

    @Override
    public Object nullSafeGet(ResultSet rs, String[] names, SessionImplementor session, Object owner) throws HibernateException, SQLException {
        Array sqlArr = rs.getArray(names[0]);
        final List result = sqlArr == null ? null : Lists.newArrayList((Object[]) sqlArr.getArray());
        return result;
    }

    @Override
    public Object assemble(final Serializable cached, final Object owner) throws HibernateException {
        return deepCopy(cached);
    }


    @Override
    public Serializable disassemble(final Object o) throws HibernateException {
        return (Serializable) deepCopy(o);
    }

    @Override
    public boolean equals(final Object x, final Object y) throws HibernateException {
        return x == null ? y == null : x.equals(y);
    }

    @Override
    public int hashCode(final Object o) throws HibernateException {
    	return ((List)o).hashCode();
    }

    @Override
    public Object deepCopy(Object o) throws HibernateException {
        return o == null ? null : Lists.newArrayList((List)o);
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public Object replace(final Object original, final Object target, final Object owner) throws HibernateException {
    	return original == null ? null: deepCopy(original);
    }

    public abstract Array createArray(final List<T> object, Connection connection) throws SQLException;

    @Override
    public int[] sqlTypes() {
        return new int[]{Types.ARRAY};
    }
}
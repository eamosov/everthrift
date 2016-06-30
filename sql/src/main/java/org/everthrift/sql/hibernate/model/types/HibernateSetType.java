package org.everthrift.sql.hibernate.model.types;

import java.io.Serializable;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.usertype.UserType;

import com.google.common.collect.Sets;


public abstract class HibernateSetType<T> implements UserType {

    @Override
    public Class<Set> returnedClass() {
        return Set.class;
    }

    @Override
    public void nullSafeSet(final PreparedStatement statement, final Object object, final int i, final SessionImplementor sessionImplementor) throws HibernateException, SQLException {
        statement.setArray(i, object == null ? null : createArray((Set<T>) object, statement.getConnection()));
    }

    @Override
    public Object nullSafeGet(ResultSet rs, String[] names, SessionImplementor session, Object owner) throws HibernateException, SQLException {
        Array sqlArr = rs.getArray(names[0]);
        final Set result = sqlArr == null ? null : Sets.newHashSet((Object[]) sqlArr.getArray());
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
        return ((Set)o).hashCode();
    }

    @Override
    public Object deepCopy(Object o) throws HibernateException {
        return o == null ? null : Sets.newHashSet((Set)o);
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public Object replace(final Object original, final Object target, final Object owner) throws HibernateException {
        return original == null ? null: deepCopy(original);
    }

    public abstract Array createArray(final Set<T> object, Connection connection) throws SQLException;

    @Override
    public int[] sqlTypes() {
        return new int[]{Types.ARRAY};
    }
}
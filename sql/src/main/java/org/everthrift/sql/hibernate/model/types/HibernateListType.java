package org.everthrift.sql.hibernate.model.types;

import com.google.common.collect.Lists;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public abstract class HibernateListType<T> implements UserType {

    @NotNull
    @Override
    public Class<List> returnedClass() {
        return List.class;
    }

    @Override
    public void nullSafeSet(@NotNull final PreparedStatement statement, @Nullable final Object object, final int i,
                            final SharedSessionContractImplementor sessionImplementor) throws HibernateException, SQLException {
        statement.setArray(i, object == null ? null : createArray((List<T>) object, statement.getConnection()));
    }

    @Nullable
    @Override
    public Object nullSafeGet(@NotNull ResultSet rs, String[] names, SharedSessionContractImplementor session,
                              Object owner) throws HibernateException, SQLException {
        Array sqlArr = rs.getArray(names[0]);
        final List result = sqlArr == null ? null : Lists.newArrayList((Object[]) sqlArr.getArray());
        return result;
    }

    @Nullable
    @Override
    public Object assemble(final Serializable cached, final Object owner) throws HibernateException {
        return deepCopy(cached);
    }

    @Override
    public Serializable disassemble(final Object o) throws HibernateException {
        return (Serializable) deepCopy(o);
    }

    @Override
    public boolean equals(@Nullable final Object x, @Nullable final Object y) throws HibernateException {
        return x == null ? y == null : x.equals(y);
    }

    @Override
    public int hashCode(@NotNull final Object o) throws HibernateException {
        return ((List) o).hashCode();
    }

    @Nullable
    @Override
    public Object deepCopy(@Nullable Object o) throws HibernateException {
        return o == null ? null : Lists.newArrayList((List) o);
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Nullable
    @Override
    public Object replace(@Nullable final Object original, final Object target, final Object owner) throws HibernateException {
        return original == null ? null : deepCopy(original);
    }

    public abstract Array createArray(final List<T> object, Connection connection) throws SQLException;

    @NotNull
    @Override
    public int[] sqlTypes() {
        return new int[]{Types.ARRAY};
    }
}
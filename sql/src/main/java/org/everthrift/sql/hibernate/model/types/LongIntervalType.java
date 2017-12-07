package org.everthrift.sql.hibernate.model.types;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.postgresql.util.PGobject;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class LongIntervalType implements UserType {

    public LongIntervalType() {

    }

    @NotNull
    @Override
    public int[] sqlTypes() {
        return new int[]{Types.OTHER};
    }

    @NotNull
    @Override
    public Class returnedClass() {
        return Long.class;
    }

    @Override
    public boolean equals(@Nullable Object x, @Nullable Object y) throws HibernateException {
        if (x == null && y == null) {
            return true;
        }

        if ((x == null && y != null) || (x != null && y == null)) {
            return false;
        }

        return x.equals(y);
    }

    @Override
    public int hashCode(@NotNull Object x) throws HibernateException {
        return x.hashCode();
    }

    @NotNull
    @Override
    public Object nullSafeGet(@NotNull ResultSet rs, String[] names, SharedSessionContractImplementor session,
                              Object owner) throws HibernateException, SQLException {
        return rs.getLong(names[0]) * 1000;
    }

    @Override
    public void nullSafeSet(@NotNull PreparedStatement st, @Nullable Object value, int index,
                            SharedSessionContractImplementor session) throws HibernateException, SQLException {

        final PGobject o = new PGobject();
        o.setType("interval");
        o.setValue(value == null ? null : ((Long) value / 1000) + " seconds");
        st.setObject(index, o);
    }

    @Override
    public Object deepCopy(Object value) throws HibernateException {
        return value;
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @NotNull
    @Override
    public Serializable disassemble(@NotNull Object value) throws HibernateException {
        return (Serializable) value;
    }

    @Override
    public Object assemble(Serializable cached, Object owner) throws HibernateException {
        return cached;
    }

    @Override
    public Object replace(Object original, Object target, Object owner) throws HibernateException {
        return original;
    }

}

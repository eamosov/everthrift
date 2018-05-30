package org.everthrift.sql.hibernate.model.types;

import org.apache.thrift.TEnum;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public abstract class TEnumType<T extends TEnum> implements UserType {

    @NotNull
    protected abstract Class<T> getTEnumClass();

    private final Method findByValue;

    public TEnumType() {
        try {
            findByValue = getTEnumClass().getMethod("findByValue", Integer.TYPE);
        } catch (@NotNull NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    @Override
    public int[] sqlTypes() {
        return new int[]{Types.INTEGER};
    }

    @NotNull
    @Override
    public Class returnedClass() {
        return getTEnumClass();
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
    public int hashCode(@Nullable Object x) throws HibernateException {
        if (x == null) {
            return 0;
        }

        return x.hashCode();
    }

    @Nullable
    @Override
    public Object nullSafeGet(@NotNull ResultSet rs, String[] names, SharedSessionContractImplementor session,
                              Object owner) throws HibernateException, SQLException {

        final Integer value = (Integer) rs.getObject(names[0]);

        if (value == null) {
            return null;
        }

        try {
            return findByValue.invoke(null, value.intValue());
        } catch (@NotNull IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void nullSafeSet(@NotNull PreparedStatement st, @Nullable Object value, int index,
                            SharedSessionContractImplementor session) throws HibernateException, SQLException {
        if (value == null) {
            st.setNull(index, java.sql.Types.INTEGER);
        } else {
            st.setInt(index, ((TEnum) value).getValue());
        }
    }

    @Override
    public Object deepCopy(Object value) throws HibernateException {
        return value;
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    @Nullable
    public Serializable disassemble(Object value) throws HibernateException {
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

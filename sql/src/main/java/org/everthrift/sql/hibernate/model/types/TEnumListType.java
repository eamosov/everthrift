package org.everthrift.sql.hibernate.model.types;

import com.google.common.collect.Lists;
import org.apache.thrift.TEnum;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public abstract class TEnumListType<T extends TEnum> implements UserType {

    @NotNull
    protected abstract Class<T> getTEnumClass();

    private final Method findByValue;

    public TEnumListType() {
        try {
            findByValue = getTEnumClass().getMethod("findByValue", Integer.TYPE);
        } catch (@NotNull NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    @Override
    public int[] sqlTypes() {
        return new int[]{Types.ARRAY};
    }

    @NotNull
    @Override
    public Class returnedClass() {
        return List.class;
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

        final Array sqlArr = rs.getArray(names[0]);
        final List values = sqlArr == null ? null : Arrays.stream((Integer[]) sqlArr.getArray())
                                                          .map(v -> {
                                                              try {
                                                                  return findByValue.invoke(null, v.intValue());
                                                              } catch (@NotNull IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                                                                  throw new RuntimeException(e);
                                                              }
                                                          })
                                                          .collect(Collectors.toList());

        return values;
    }

    @Override
    public void nullSafeSet(@NotNull PreparedStatement st, @Nullable Object value, int index,
                            SharedSessionContractImplementor session) throws HibernateException, SQLException {

        st.setArray(index, value == null ? null : st.getConnection()
                                                    .createArrayOf("integer", ((List<TEnum>) value).stream()
                                                                                                   .map(TEnum::getValue)
                                                                                                   .toArray()));
    }

    @Nullable
    @Override
    public Object assemble(final Serializable cached, final Object owner) throws HibernateException {
        return deepCopy(cached);
    }

    @NotNull
    @Override
    public Serializable disassemble(final Object o) throws HibernateException {
        return (Serializable) deepCopy(o);
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

}

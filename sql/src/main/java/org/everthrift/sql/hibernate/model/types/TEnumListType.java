package org.everthrift.sql.hibernate.model.types;

import com.google.common.collect.Lists;
import org.apache.thrift.TEnum;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;

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

    protected abstract Class<T> getTEnumClass();

    private final Method findByValue;

    public TEnumListType() {
        try {
            findByValue = getTEnumClass().getMethod("findByValue", Integer.TYPE);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int[] sqlTypes() {
        return new int[]{Types.ARRAY};
    }

    @Override
    public Class returnedClass() {
        return List.class;
    }

    @Override
    public boolean equals(Object x, Object y) throws HibernateException {
        if (x == null && y == null) {
            return true;
        }

        if ((x == null && y != null) || (x != null && y == null)) {
            return false;
        }

        return x.equals(y);
    }

    @Override
    public int hashCode(Object x) throws HibernateException {
        if (x == null) {
            return 0;
        }

        return x.hashCode();
    }

    @Override
    public Object nullSafeGet(ResultSet rs, String[] names, SharedSessionContractImplementor session,
                              Object owner) throws HibernateException, SQLException {

        final Array sqlArr = rs.getArray(names[0]);
        final List values = sqlArr == null ? null : Arrays.stream((Integer[]) sqlArr.getArray())
                                                          .map(v -> {
                                                              try {
                                                                  return findByValue.invoke(null, v.intValue());
                                                              } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                                                                  throw new RuntimeException(e);
                                                              }
                                                          })
                                                          .collect(Collectors.toList());

        return values;
    }

    @Override
    public void nullSafeSet(PreparedStatement st, Object value, int index,
                            SharedSessionContractImplementor session) throws HibernateException, SQLException {

        st.setArray(index, value == null ? null : st.getConnection()
                                                    .createArrayOf("integer", ((List<TEnum>) value).stream()
                                                                                                   .map(TEnum::getValue)
                                                                                                   .toArray()));
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
    public Object deepCopy(Object o) throws HibernateException {
        return o == null ? null : Lists.newArrayList((List) o);
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public Object replace(final Object original, final Object target, final Object owner) throws HibernateException {
        return original == null ? null : deepCopy(original);
    }

}

package org.everthrift.sql.hibernate.model.types;

import com.google.common.collect.Maps;
import org.everthrift.utils.SqlUtils;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public class LongLongHstoreType implements UserType {

    @NotNull
    @Override
    public int[] sqlTypes() {
        return new int[]{Types.OTHER};
    }

    @NotNull
    @Override
    public Class returnedClass() {
        return Map.class;
    }

    @Override
    public boolean equals(@Nullable Object x, @Nullable Object y) throws HibernateException {

        if (x == null && y == null) {
            return true;
        }

        if ((x == null && y != null) || (x != null && y == null)) {
            return false;
        }

        return ((Map) x).equals(y);
    }

    @Override
    public int hashCode(@Nullable Object x) throws HibernateException {

        if (x == null || ((Map) x).size() == 0) {
            return 0;
        }

        return ((Map) x).hashCode();
    }

    @Nullable
    @Override
    public Object nullSafeGet(@NotNull ResultSet rs, String[] names, SharedSessionContractImplementor session,
                              Object owner) throws HibernateException, SQLException {

        final Map<String, String> hstore = (Map<String, String>) rs.getObject(names[0]);

        if (hstore == null) {
            return null;
        }

        if (hstore.isEmpty()) {
            return new HashMap();
        }

        final Map<Long, Long> ret = Maps.newHashMap();
        for (Map.Entry<String, String> e : hstore.entrySet()) {
            ret.put(Long.parseLong(e.getKey()), Long.parseLong(e.getValue()));
        }

        return ret;
    }

    @Override
    public void nullSafeSet(@NotNull PreparedStatement st, @Nullable Object value, int index,
                            SharedSessionContractImplementor session) throws HibernateException, SQLException {
        st.setString(index, value == null ? null : (String) SqlUtils.toSqlParam((Map) value));
    }

    @Nullable
    @Override
    public Object deepCopy(@Nullable Object value) throws HibernateException {
        return value == null ? null : new HashMap((Map) value);
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Nullable
    @Override
    public Object assemble(final Serializable cached, final Object owner) throws HibernateException {
        return deepCopy(cached);
    }

    @Nullable
    @Override
    public Serializable disassemble(final Object o) throws HibernateException {
        return (Serializable) deepCopy(o);
    }

    @Nullable
    @Override
    public Object replace(@Nullable Object original, Object target, Object owner) throws HibernateException {
        return original == null ? null : deepCopy(original);
    }

}

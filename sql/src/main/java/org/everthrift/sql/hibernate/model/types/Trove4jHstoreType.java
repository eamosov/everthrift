package org.everthrift.sql.hibernate.model.types;

import gnu.trove.impl.hash.THash;
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
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class Trove4jHstoreType<T extends THash> implements UserType {

    @NotNull
    @Override
    public int[] sqlTypes() {
        return new int[]{Types.OTHER};
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

        if (x == null || ((T) x).size() == 0) {
            return 0;
        }

        return x.hashCode();
    }

    @NotNull
    protected abstract T transform(Map<String, String> input);

    @NotNull
    protected abstract Map transformReverse(T input);

    @Nullable
    @Override
    public Object nullSafeGet(@NotNull ResultSet rs, String[] names, SharedSessionContractImplementor session,
                              Object owner) throws HibernateException, SQLException {

        final Map<String, String> hstore = (Map<String, String>) rs.getObject(names[0]);

        if (hstore == null) {
            return null;
        }

        return transform(hstore);
    }

    @Override
    public void nullSafeSet(@NotNull PreparedStatement st, @Nullable Object value, int index,
                            SharedSessionContractImplementor session) throws HibernateException, SQLException {
        st.setString(index, value == null ? null : (String) SqlUtils.toSqlParam(transformReverse((T) value)));
    }

    // @Override
    // public Object deepCopy(Object value) throws HibernateException {
    // return value==null?new HashMap<>():new HashMap((Map)value);
    // }

    @Override
    public boolean isMutable() {
        return true;
    }

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
    public Object replace(@Nullable Object original, Object target, Object owner) throws HibernateException {
        return original == null ? null : deepCopy(original);
    }

}

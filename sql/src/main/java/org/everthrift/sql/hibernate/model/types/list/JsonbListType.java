package org.everthrift.sql.hibernate.model.types.list;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.everthrift.sql.hibernate.model.types.CustomUserType;
import org.everthrift.utils.TypeUtils;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.postgresql.util.PGobject;
import org.springframework.beans.BeanUtils;

import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Objects;

/**
 * Created by fluder on 10.04.17.
 */
public abstract class JsonbListType implements CustomUserType {

    final private Gson gson = new GsonBuilder().create();

    @NotNull
    protected abstract Type getListType();

    protected Gson gson() {
        return gson;
    }

    @Override
    public boolean accept(Class entityClass, @NotNull Class propertyClass, String propertyName, int jdbcTypeId, @NotNull String jdbcColumnType, String columnName) {

        if (!List.class.isAssignableFrom(propertyClass)) {
            return false;
        }

        if (!jdbcColumnType.equalsIgnoreCase("jsonb")) {
            return false;
        }

        final PropertyDescriptor pd = BeanUtils.getPropertyDescriptor(entityClass, propertyName);
        if (pd == null) {
            return false;
        }

        return TypeUtils.isAssignable(getListType(), pd.getReadMethod().getGenericReturnType());
    }


    @NotNull
    @Override
    public int[] sqlTypes() {
        return new int[]{Types.OTHER};
    }

    @NotNull
    @Override
    public Class returnedClass() {
        return List.class;
    }

    @Override
    public boolean equals(Object x, Object y) throws HibernateException {
        return Objects.equals(x, y);
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
    public Object nullSafeGet(@NotNull ResultSet rs, String[] names, SharedSessionContractImplementor session, Object owner) throws HibernateException, SQLException {
        final PGobject value = (PGobject) rs.getObject(names[0]);

        if (value == null) {
            return null;
        }

        return gson().fromJson(value.getValue(), getListType());
    }

    @Override
    public void nullSafeSet(@NotNull PreparedStatement st, @Nullable Object value, int index, SharedSessionContractImplementor session) throws HibernateException, SQLException {
        if (value == null) {
            st.setNull(index, Types.OTHER);
            return;
        }

        final PGobject o = new PGobject();
        o.setType("jsonb");
        o.setValue(gson().toJson(value, getListType()));
        st.setObject(index, o, Types.OTHER);
    }

    @Nullable
    @Override
    public Object deepCopy(@Nullable Object value) throws HibernateException {

        if (value == null) {
            return null;
        }

        return Lists.newArrayList((List) value);
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Nullable
    @Override
    public Serializable disassemble(Object value) throws HibernateException {
        return (Serializable) deepCopy(value);
    }

    @Nullable
    @Override
    public Object assemble(Serializable cached, Object owner) throws HibernateException {
        return deepCopy(cached);
    }

    @Nullable
    @Override
    public Object replace(@Nullable Object original, Object target, Object owner) throws HibernateException {
        return original == null ? null : deepCopy(original);
    }
}

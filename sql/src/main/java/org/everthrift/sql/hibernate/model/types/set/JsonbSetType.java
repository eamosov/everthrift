package org.everthrift.sql.hibernate.model.types.set;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.everthrift.sql.hibernate.model.types.CustomUserType;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.postgresql.util.PGobject;
import org.springframework.beans.BeanUtils;

import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Objects;
import java.util.Set;

/**
 * Created by fluder on 10.04.17.
 */
public abstract class JsonbSetType implements CustomUserType {

    final private Gson gson = new GsonBuilder().create();

    protected abstract Type getSetType();

    protected Gson gson(){
        return gson;
    }

    @Override
    public final boolean accept(Class entityClass, Class propertyClass, String propertyName, int jdbcTypeId, String jdbcColumnType, String columnName) {

        if (!Set.class.isAssignableFrom(propertyClass)) {
            return false;
        }

        if (!jdbcColumnType.equalsIgnoreCase("jsonb")) {
            return false;
        }

        final PropertyDescriptor pd = BeanUtils.getPropertyDescriptor(entityClass, propertyName);
        if (pd == null) {
            return false;
        }

        return pd.getReadMethod().getGenericReturnType().toString().equals(getSetType().toString());
    }


    @Override
    public int[] sqlTypes() {
        return new int[]{Types.OTHER};
    }

    @Override
    public Class returnedClass() {
        return Set.class;
    }

    @Override
    public boolean equals(Object x, Object y) throws HibernateException {
        return Objects.equals(x, y);
    }

    @Override
    public int hashCode(Object x) throws HibernateException {
        if (x == null) {
            return 0;
        }
        return x.hashCode();
    }

    @Override
    public Object nullSafeGet(ResultSet rs, String[] names, SharedSessionContractImplementor session, Object owner) throws HibernateException, SQLException {
        final PGobject value = (PGobject) rs.getObject(names[0]);

        if (value == null) {
            return null;
        }

        return gson.fromJson(value.getValue(), getSetType());
    }

    @Override
    public void nullSafeSet(PreparedStatement st, Object value, int index, SharedSessionContractImplementor session) throws HibernateException, SQLException {
        if (value == null) {
            st.setNull(index, Types.OTHER);
            return;
        }

        final PGobject o = new PGobject();
        o.setType("jsonb");
        o.setValue(gson.toJson(value, getSetType()));
        st.setObject(index, o, Types.OTHER);
    }

    @Override
    public Object deepCopy(Object value) throws HibernateException {

        if (value == null) {
            return null;
        }

        return Sets.newHashSet((Set) value);
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public Serializable disassemble(Object value) throws HibernateException {
        return (Serializable) deepCopy(value);
    }

    @Override
    public Object assemble(Serializable cached, Object owner) throws HibernateException {
        return deepCopy(cached);
    }

    @Override
    public Object replace(Object original, Object target, Object owner) throws HibernateException {
        return original == null ? null : deepCopy(original);
    }
}

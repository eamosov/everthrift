package org.everthrift.sql.hibernate.model.types;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.apache.thrift.TBase;
import org.everthrift.appserver.utils.thrift.GsonSerializer.TBaseSerializer;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Objects;

@SuppressWarnings({"unchecked"})
public abstract class JsonType implements UserType {

    private static final Logger log = LoggerFactory.getLogger(JsonType.class);

    private final Gson gson = new GsonBuilder().registerTypeHierarchyAdapter(TBase.class, new TBaseSerializer())
                                               .create();

    @NotNull
    @Override
    public int[] sqlTypes() {
        return new int[]{Types.OTHER};
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
    public Object nullSafeGet(@NotNull ResultSet rs, String[] names, SharedSessionContractImplementor session,
                              Object owner) throws HibernateException, SQLException {

        final PGobject json = (PGobject) rs.getObject(names[0]);

        if (json == null) {
            return null;
        }

        try {
            return gson.fromJson(json.getValue(), returnedClass());
        } catch (JsonSyntaxException e) {
            log.error("Coudn't parse '{}' in {}", json.getValue(), this.returnedClass().getSimpleName());
            throw new HibernateException(e);
        }
    }

    @Override
    public void nullSafeSet(@NotNull PreparedStatement st, @Nullable Object value, int index,
                            SharedSessionContractImplementor session) throws HibernateException, SQLException {

        final PGobject json = new PGobject();
        json.setType("jsonb");
        json.setValue(value == null ? null : gson.toJson(value));
        st.setObject(index, json);
    }

    @Nullable
    @Override
    public Object deepCopy(@Nullable Object value) throws HibernateException {
        try {
            return value == null ? null : value.getClass().getConstructor(value.getClass()).newInstance(value);
        } catch (@NotNull InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
            | NoSuchMethodException | SecurityException e) {
            throw new HibernateException(e);
        }
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

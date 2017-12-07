package org.everthrift.sql.hibernate.model.types.map;

import com.google.common.base.Throwables;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.thrift.TBase;
import org.everthrift.appserver.utils.thrift.GsonSerializer;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.postgresql.util.PGobject;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by fluder on 10.04.17.
 */
public abstract class ObjStructMapType<T extends TBase> extends JsonbMapType {

    @NotNull
    protected abstract Class<T> getStructClass();

    private final Constructor<T> copyConstructor;

    final private Gson gson = new GsonBuilder().registerTypeHierarchyAdapter(TBase.class, new GsonSerializer.TBaseSerializer(FieldNamingPolicy.IDENTITY, true))
                                               .create();


    public ObjStructMapType() {
        try {
            copyConstructor = getStructClass().getConstructor(getStructClass());
        } catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    @Nullable
    @Override
    public Object nullSafeGet(@NotNull ResultSet rs, String[] names, SharedSessionContractImplementor session, Object owner) throws HibernateException, SQLException {
        final PGobject value = (PGobject) rs.getObject(names[0]);

        if (value == null) {
            return null;
        }

        return gson.fromJson(value.getValue(), getMapType());
    }

    @Override
    public void nullSafeSet(@NotNull PreparedStatement st, @Nullable Object value, int index, SharedSessionContractImplementor session) throws HibernateException, SQLException {
        if (value == null) {
            st.setNull(index, Types.OTHER);
            return;
        }

        final PGobject o = new PGobject();
        o.setType("jsonb");
        o.setValue(gson.toJson(value, getMapType()));
        st.setObject(index, o, Types.OTHER);
    }


    @Nullable
    @Override
    public Object deepCopy(@Nullable Object value) throws HibernateException {

        if (value == null) {
            return null;
        }

        return ((Map<?, ?>) value).entrySet()
                                  .stream()
                                  .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                                      try {
                                          return copyConstructor.newInstance(e.getValue());
                                      } catch (@NotNull InstantiationException | IllegalAccessException | InvocationTargetException e1) {
                                          throw Throwables.propagate(e1);
                                      }
                                  }));
    }

}

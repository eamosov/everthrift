package org.everthrift.sql.hibernate.model.types.set;

import com.google.common.base.Throwables;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.thrift.TBase;
import org.everthrift.appserver.utils.thrift.GsonSerializer;
import org.everthrift.sql.hibernate.model.types.list.JsonbListType;
import org.hibernate.HibernateException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by fluder on 10.04.17.
 */
public abstract class ObjSetType<T> extends JsonbSetType {

    @NotNull
    protected abstract Class<T> getStructClass();

    private final Constructor<T> copyConstructor;

    final private Gson gson = new GsonBuilder().registerTypeHierarchyAdapter(TBase.class, new GsonSerializer.TBaseSerializer(FieldNamingPolicy.IDENTITY, true))
                                               .create();

    @Override
    protected Gson gson() {
        return gson;
    }

    public ObjSetType() {
        try {
            copyConstructor = getStructClass().getConstructor(getStructClass());
        } catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    @Nullable
    @Override
    public Object deepCopy(@Nullable Object value) throws HibernateException {

        if (value == null) {
            return null;
        }

        return ((Set<T>) value).stream().map(i -> {
            try {
                return copyConstructor.newInstance(i);
            } catch (@NotNull InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw Throwables.propagate(e);
            }
        }).collect(Collectors.toSet());
    }

}

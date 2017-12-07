package org.everthrift.appserver.utils.thrift;

import com.google.common.base.Throwables;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.everthrift.utils.Pair;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ThriftUtils {

    private static final Object NULL_RESULT = new Object();

    private static final AtomicReference<Reference2ObjectMap<Class, Object>> classes = new AtomicReference<Reference2ObjectMap<Class, Object>>(new Reference2ObjectOpenHashMap<Class, Object>());

    @SuppressWarnings({"rawtypes", "unchecked"})
    @NotNull
    public static Pair<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>> getRootThriftClass(Class<? extends TBase> cls) {

        Reference2ObjectMap<Class, Object> _classes = classes.get();
        Object p = _classes.get(cls);

        if (p == null) {
            p = _getRootThriftClass(cls);
            if (p == null) {
                p = NULL_RESULT;
            }

            _classes = new Reference2ObjectOpenHashMap<Class, Object>(_classes);
            _classes.put(cls, p);
            classes.set(_classes);
        }

        if (p == NULL_RESULT) {
            return null;
        }

        return (Pair<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>>) p;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Pair<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>> _getRootThriftClass(Class<? extends TBase> cls) {
        Map<? extends TFieldIdEnum, FieldMetaData> map = null;
        Class<? extends TBase> nextThriftClass = cls;
        Class<? extends TBase> thriftClass;
        do {
            thriftClass = nextThriftClass;
            try {
                map = FieldMetaData.getStructMetaDataMap(thriftClass);
            } catch (Exception e) {
                map = null;
            }
            nextThriftClass = (Class<? extends TBase>) thriftClass.getSuperclass();
        } while (map == null && nextThriftClass != null);

        if (map == null) {
            return null;
        }

        return Pair.<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>>create(thriftClass, map);
    }

    public static <S extends TBase, D extends S> void copyFields(@NotNull S from, @NotNull D to, @NotNull TFieldIdEnum... fields) {
        for (int i = 0; i < fields.length; i++) {
            final TFieldIdEnum id = fields[i];
            to.setFieldValue(id, from.isSet(id) ? from.getFieldValue(id) : null);
        }
    }

    public static <S extends TBase, D extends S> void updateIsSetFields(@NotNull S from, @NotNull D to, @NotNull TFieldIdEnum... fields) {
        for (int i = 0; i < fields.length; i++) {
            final TFieldIdEnum id = fields[i];
            if (from.isSet(id)) {
                to.setFieldValue(id, from.getFieldValue(id));
            }
        }
    }

    public static void defaultBooleanFields(@NotNull TBase src, @NotNull TFieldIdEnum... fields) {
        for (TFieldIdEnum id : fields) {
            if (!src.isSet(id)) {
                src.setFieldValue(id, false);
            }
        }
    }

    @NotNull
    public static TFieldIdEnum[] getFieldIds(@NotNull TBase tBase) {
        Objects.requireNonNull(tBase);
        return getFieldIds(tBase.getClass());
    }

    @NotNull
    public static TFieldIdEnum[] getFieldIds(@NotNull Class<? extends TBase> cls) {
        try {
            return (TFieldIdEnum[]) Class.forName(cls.getCanonicalName() + "$_Fields").getMethod("values").invoke(null);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @NotNull
    public static TFieldIdEnum getFieldId(@NotNull Class<? extends TBase> cls, String name) {
        try {
            return (TFieldIdEnum) Class.forName(cls.getCanonicalName() + "$_Fields")
                                       .getMethod("findByName", String.class)
                                       .invoke(null, name);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private static final Pattern pattern = Pattern.compile("(^.*\\.([^\\.]+))\\$_Fields$");

    public static String getStructName(@NotNull TFieldIdEnum id) {
        final Matcher matcher = pattern.matcher(id.getClass().getName());
        if (matcher.matches()) {
            return matcher.group(2);
        } else {
            return null;
        }
    }

    public static String getStructFullName(@NotNull TFieldIdEnum id) {
        final Matcher matcher = pattern.matcher(id.getClass().getName());
        if (matcher.matches()) {
            return matcher.group(1);
        } else {
            return null;
        }
    }

    public static String getFieldName(@NotNull TFieldIdEnum id) {
        final String structName = getStructName(id);
        if (structName != null) {
            return structName + "." + id.getFieldName();
        } else {
            return id.getFieldName();
        }
    }
}

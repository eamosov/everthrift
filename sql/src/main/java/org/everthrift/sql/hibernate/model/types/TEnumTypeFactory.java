package org.everthrift.sql.hibernate.model.types;

import com.google.common.collect.Maps;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;
import org.apache.thrift.TEnum;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

@SuppressWarnings("rawtypes")
public class TEnumTypeFactory {

    private static final Map<Class<? extends TEnum>, Class<? extends TEnumType>> map = Maps.newIdentityHashMap();

    public static synchronized Class<? extends TEnumType> create(@NotNull Class<? extends TEnum> enumType) {

        Class<? extends TEnumType> cls = map.get(enumType);
        if (cls != null) {
            return cls;
        }

        cls = getTEnumType(enumType);
        map.put(enumType, cls);
        return cls;
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends TEnumType> getTEnumType(@NotNull Class<? extends TEnum> enumType) {
        final ClassPool pool = ClassPool.getDefault();
        final CtClass cc = pool.makeClass("org.everthrift.hibernate.model.types.generated." + enumType.getSimpleName() + "EnumType");
        try {
            cc.setSuperclass(pool.get(TEnumType.class.getName()));
            cc.addMethod(CtMethod.make("protected Class getTEnumClass() { try { return (Class)(Thread.currentThread().getContextClassLoader().loadClass(\""
                                           + enumType.getName() + "\")); } catch (ClassNotFoundException e) {throw new RuntimeException(e);}}",
                                       cc));
            return cc.toClass();
        } catch (@NotNull CannotCompileException | NotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}

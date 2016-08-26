package org.everthrift.thrift;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public interface TBaseHasModel<M extends TBaseModel<?, ?>> {

    static <M extends TBaseModel<?, ?>> Class<M> getModel(Class<? extends TBaseHasModel> cls) {

        if (!TBaseHasModel.class.isAssignableFrom(cls)) {
            return null;
        }

        for (Type t : cls.getGenericInterfaces()) {

            if (t instanceof ParameterizedType && TBaseHasModel.class.isAssignableFrom(((Class) (((ParameterizedType) t)
                .getRawType())))) {

                final Type h = ((ParameterizedType) t).getActualTypeArguments()[0];
                return Class.class.isInstance(h) ? (Class) h : (Class) ((ParameterizedType) h).getRawType();
            }
        }
        return null;
    }

    default Class<M> getModel() {
        return getModel(this.getClass());
    }
}

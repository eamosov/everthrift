package org.everthrift.sql.hibernate.model.types.map;

import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Created by fluder on 10.04.17.
 */
public class IntStringMapType extends JsonbMapType {

    final TypeToken<Map<Integer, String>> fieldTypeToken = new TypeToken<Map<Integer, String>>() {
    };

    @Override
    protected Type getMapType() {
        return fieldTypeToken.getType();
    }

}

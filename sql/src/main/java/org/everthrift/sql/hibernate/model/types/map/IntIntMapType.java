package org.everthrift.sql.hibernate.model.types.map;

import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Created by fluder on 10.04.17.
 */
public class IntIntMapType extends JsonbMapType {

    final TypeToken<Map<Integer, Integer>> fieldTypeToken = new TypeToken<Map<Integer, Integer>>() {
    };

    @Override
    protected Type getMapType() {
        return fieldTypeToken.getType();
    }

}

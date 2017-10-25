package org.everthrift.sql.hibernate.model.types.map;

import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Created by fluder on 10.04.17.
 */
public class StringIntMapType extends JsonbMapType {

    final TypeToken<Map<String, Integer>> fieldTypeToken = new TypeToken<Map<String, Integer>>() {
    };

    @Override
    protected Type getMapType() {
        return fieldTypeToken.getType();
    }

}

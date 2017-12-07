package org.everthrift.sql.hibernate.model;

import org.everthrift.sql.hibernate.model.types.CustomUserType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by fluder on 11.03.17.
 */
public class CustomTypesRegistry {

    private static final CustomTypesRegistry INSTANCE = new CustomTypesRegistry();

    private final List<CustomUserType> userTypes = new ArrayList<>();

    @NotNull
    public static CustomTypesRegistry getInstance(){
        return INSTANCE;
    }

    private CustomTypesRegistry(){

    }

    public synchronized  void register(CustomUserType userType){
        userTypes.add(userType);
    }

    @Nullable
    public synchronized String get(Class entityClass, Class propertyClass, String propertyName, int jdbcTypeId, String jdbcColumnType, String columnName){

        for (CustomUserType c: userTypes){
            if (c.accept(entityClass, propertyClass, propertyName, jdbcTypeId, jdbcColumnType, columnName))
                return c.getClass().getCanonicalName();
        }

        return null;
    }
}

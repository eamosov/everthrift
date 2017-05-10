package org.everthrift.sql.hibernate.model.types;

import org.hibernate.usertype.UserType;

/**
 * Created by fluder on 11.03.17.
 */
public interface CustomUserType extends UserType {

    boolean accept(Class entityClass, Class propertyClass, String propertyName, int jdbcTypeId, String jdbcColumnType, String columnName);
}

package org.everthrift.sql.hibernate;

import org.hibernate.property.access.spi.PropertyAccess;
import org.hibernate.property.access.spi.PropertyAccessStrategy;
import org.jetbrains.annotations.NotNull;

public class ThriftPropertyAccessStrategy implements PropertyAccessStrategy {

    public ThriftPropertyAccessStrategy() {

    }

    @NotNull
    @Override
    public PropertyAccess buildPropertyAccess(@NotNull Class containerJavaType, String propertyName) {
        return new ThriftPropertyAccess(this, containerJavaType, propertyName);
    }

}

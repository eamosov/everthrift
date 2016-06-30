package org.everthrift.sql.hibernate;

import org.hibernate.property.access.spi.PropertyAccess;
import org.hibernate.property.access.spi.PropertyAccessStrategy;

public class ThriftPropertyAccessStrategy implements PropertyAccessStrategy {

    public ThriftPropertyAccessStrategy() {

    }

    @Override
    public PropertyAccess buildPropertyAccess(Class containerJavaType, String propertyName) {
        return new ThriftPropertyAccess(this, containerJavaType, propertyName);
    }

}

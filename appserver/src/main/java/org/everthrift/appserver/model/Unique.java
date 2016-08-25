package org.everthrift.appserver.model;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Repeatable(UniqueKeys.class)
@Retention(RetentionPolicy.RUNTIME)
public @interface Unique {
    String value();

    String clause() default "";
}

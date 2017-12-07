package org.everthrift.appserver.model;

import org.jetbrains.annotations.NotNull;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Repeatable(UniqueKeys.class)
@Retention(RetentionPolicy.RUNTIME)
public @interface Unique {
    @NotNull String value();

    @NotNull String clause() default "";
}

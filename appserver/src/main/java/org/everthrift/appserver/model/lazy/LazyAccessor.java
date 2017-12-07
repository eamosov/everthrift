package org.everthrift.appserver.model.lazy;

import org.jetbrains.annotations.NotNull;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface LazyAccessor {
    @NotNull String[] value() default ""; // scenarios "default", "admin", "json" ...
}

package org.everthrift.appserver.model.lazy;

import org.jetbrains.annotations.NotNull;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * examples:
 *
 * @author fluder
 * @LazyMethod public AddressLineModel loadAddress(Registry r, Object parent){
 * ...//нужно присвоить значение свойству "address" и вернуть этот объект }
 * @LazyMethod public void loadAddress(Registry r, Object parent){ }
 * @LazyMethod public void loadAddress(Registry r){ ...//загрузить свойство
 * "address" через Registry в batch режиме }
 */

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface LazyMethod {
    @NotNull String[] value() default ""; // scenarios "default", "admin", "json" ...
}

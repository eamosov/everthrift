package org.everthrift.appserver.transport.tcp;

import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Аннотация на контроллер, выполняющийся в контексте ThriftServer и доступный
 * для внешнего вызова
 *
 * @author fluder
 */
@Component(value = "")
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface RpcSyncTcp {
    @NotNull String value() default "";
}

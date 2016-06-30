package org.everthrift.appserver.transport.tcp;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;


/**
 * Аннотация на контроллер, выполняющийся в контексте ThriftServer и доступный для внешнего вызова
 * @author fluder
 *
 */
@Component(value = "")
@Scope(value=ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface RpcSyncTcp{
    String value() default "";
}

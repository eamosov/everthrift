package org.everthrift.rabbit;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * По-умолчанию сервер будет слушать очередь с именем, совпадающем с именем
 * thrift сервиса. Можно переопределить имя этой очереди через свойство
 * 'rabbit.rpc.${queueName}.queue'
 * <p>
 * Клиент сообщения отправляет в Exchange c именем, совпадающем с именем thrift
 * сервиса.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public @interface RpcRabbit {
}

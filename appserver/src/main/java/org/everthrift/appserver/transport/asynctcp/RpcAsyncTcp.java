package org.everthrift.appserver.transport.asynctcp;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;


/**
 * Аннотация на контроллер, выполняющийся в контексте int-ip:tcp-inbound-channel-adapter
 * Такой контроллем имеет возможность асинхронного ответа, посредством завершения работы через
 * 
 * ThriftController.waitForAnswer() и асинхронным ответом через ThriftController.sendAnswer(TBase answer)
 * 
 * @author fluder
 *
 */
@Component
@Scope(value=ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public @interface RpcAsyncTcp {
}

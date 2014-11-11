package com.knockchat.appserver.controller;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.jgroups.blocks.ResponseMode;

import com.knockchat.appserver.transport.jgroups.RpcJGroups;

/**
 * Наличие этой аннотации на контроллере приводит к вызову сервиса на всех узлах в кластере
 * @author fluder
 *
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@RpcJGroups
public @interface RpcClustered {

	/**
	 * Если GET_NONE, то основной узел не ждет ответа от других узлов в кластере
	 * @return
	 */
	ResponseMode value() default ResponseMode.GET_NONE;
	int timeout() default 500;
}

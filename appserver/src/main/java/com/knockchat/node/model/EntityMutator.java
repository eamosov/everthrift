package com.knockchat.node.model;

import org.apache.thrift.TException;

public interface EntityMutator<ENTITY>{

	/**
	 * Перед транзакцией
	 * @return true = обновить в базе, false - не обновлять в базе и успешно завершить обновление 
	 */
	boolean beforeUpdate() throws TException;
	
	/**
	 *	Вызывается внутри транзакции 
	 * @param e
	 * @return  true = обновить в базе, false - не обновлять в базе и успешно завершить обновление
	 */
	boolean update(ENTITY e) throws TException;

	/**
	 * После транзакции в finally блоке
	 */
	void afterTransactionClosed();
	
	/**
	 * после завершения тразакции без исключений в случае, если  update() возвращает true
	 */
	void afterUpdate();
}
package com.knockchat.utils.thrift.scanner;

public interface ScenarioAwareIF {
	
	/**
	 * Возвращает поля, участвующие в обходе сканера
	 * "*" - добавление всех полей
	 * "field" - добавление поля field
	 * "!field" - исключение поля field
	 * 
	 * @param name
	 * @return
	 */
	String[] getScenario(String name);
}

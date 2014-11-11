package com.knockchat.utils.meta;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public interface MetaClass {

	/**
	 * Получить имя класса
	 * @return имя класса, описываемого метаклассом
	 */
	String getName();
	
	/**
	 * Получить имя класса в представлении JVM
	 * @return имя класса в JVM
	 */
	String getJvmName();
	
	/**
	 * Получить тип в представлении JVM
	 * @return тип в представлении JVM
	 */
	String getJvmType();
	
	/**
	 * Получить класс объекта.
	 * 
	 * @return класс объекта
	 */
	Class<?> getObjectClass();

	/**
	 * Создать новый экземпляр объекта.
	 * 
	 * @return новый экземпляр объекта
	 */
	Object newInstance();

	/**
	 * Получить метакласс для вложенного класса.
	 * 
	 * @param name
	 *            имя вложенного класса
	 * @return метакласс для вложенного класса
	 */
	MetaClass getNested( String name );
	
	/**
	 * Получить метод по имени.
	 * @param methodName имя метода
	 * @return метод
	 */
	MetaMethod getMethod( String methodName );
	
	/**
	 * Получить массив методов класса
	 * @return массив методов класса
	 */
	MetaMethod[] getMethods();
	
	/**
	 * Получить свойство по имени
	 * @param propertyName имя свойства
	 * @return свойство
	 */
	MetaProperty getProperty( String propertyName );
	
	/**
	 * Получить массив свойств класса
	 * @return массив свойств класса
	 */
	MetaProperty[] getProperties();
	
	/**
	 * Получить поле по имени.
	 * @param fieldName имя поля
	 * @return поле
	 */
	MetaProperty getFieldProperty( String fieldName );
	
	/**
	 * Получить массив полей класса
	 * @return массив полей класса
	 */
	MetaProperty[] getFieldProperties();
	
	/**
	 * Получить свойство по имени.
	 * @param propertyName имя свойства
	 * @return свойство
	 */
	MetaProperty getBeanProperty( String propertyName );
	
	/**
	 * Получить список свойств JavaBean для класса
	 * @return массив свойств
	 */
	MetaProperty[] getBeanProperties();
	
}

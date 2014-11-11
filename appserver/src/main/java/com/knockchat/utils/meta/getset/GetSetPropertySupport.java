package com.knockchat.utils.meta.getset;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.knockchat.utils.meta.MetaClass;
import com.knockchat.utils.meta.MetaMethod;
import com.knockchat.utils.meta.MetaProperty;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class GetSetPropertySupport {

	public static void get( MetaClass metaClass, Map<String,MetaProperty> properties ) {

		TreeMap<String, MetaMethod> getters = new TreeMap<String, MetaMethod>();
		TreeMap<String, MetaMethod> setters = new TreeMap<String, MetaMethod>();

		for ( MetaMethod method : metaClass.getMethods() ) {
			String methodName = method.getName();

			if ( methodName.startsWith( "get" ) ) {
				getters.put( getPropertyName( methodName, 3 ), method );
			} else if ( methodName.startsWith( "set" ) ) {
				setters.put( getPropertyName( methodName, 3 ), method );
			}else if ( methodName.startsWith( "is" ) ) {
				getters.put( getPropertyName( methodName, 2 ), method );
			} 
		}

		Iterator<Map.Entry<String, MetaMethod>> getterIt = getters.entrySet().iterator();
		Iterator<Map.Entry<String, MetaMethod>> setterIt = setters.entrySet().iterator();

		Map.Entry<String, MetaMethod> currGetter = tryGet( getterIt );
		Map.Entry<String, MetaMethod> currSetter = tryGet( setterIt );

		/*
		 * Перебираем геттеры и сеттеры паралельно в порядке возрастания
		 * ассоциированных с ними свойств. Когда встречаем пару с одинаковым
		 * ключом - формируем свойство, если пары нет - формируется свойство для
		 * метода с наименьшим ключом.
		 */
		while ( currGetter != null && currSetter != null ) {
			int res = currGetter.getKey().compareTo( currSetter.getKey() );
			if ( res < 0 ) {
				assert !properties.containsKey( currGetter.getKey() );

				properties.put( currGetter.getKey(), new GetSetProperty( currGetter.getKey(), currGetter.getValue(), null ) );
				currGetter = tryGet( getterIt );
			} else if ( res > 0 ) {
				assert !properties.containsKey( currSetter.getKey() );

				properties.put( currSetter.getKey(), new GetSetProperty( currSetter.getKey(), null, currSetter.getValue() ) );
				currSetter = tryGet( setterIt );
			} else {
				assert !properties.containsKey( currGetter.getKey() );

				properties.put( currSetter.getKey(), new GetSetProperty( currSetter.getKey(), currGetter.getValue(), currSetter.getValue() ) );
				currSetter = tryGet( setterIt );
				currGetter = tryGet( getterIt );
			}
		}

		/*
		 * Перебираем оставшиеся геттеры, из них формируем множество свойств
		 * только для чтения
		 */
		while ( currGetter != null ) {
			assert !properties.containsKey( currGetter.getKey() );

			properties.put( currGetter.getKey(), new GetSetProperty( currGetter.getKey(), currGetter.getValue(), null ) );
			currGetter = tryGet( getterIt );
		}

		/*
		 * Перебираем оставшиеся сеттеры, из них формируем множество свойств
		 * только для записи
		 */
		while ( currSetter != null ) {
			assert !properties.containsKey( currSetter.getKey() );

			properties.put( currSetter.getKey(), new GetSetProperty( currSetter.getKey(), null, currSetter.getValue() ) );
			currSetter = tryGet( setterIt );
		}
	}

	private static Entry<String, MetaMethod> tryGet( Iterator<Map.Entry<String, MetaMethod>> getterIt ) {
		return getterIt.hasNext() ? getterIt.next() : null;
	}

	private static String getPropertyName( String methodName, int prefixLength ) {
		if ( methodName.length() > prefixLength + 1 )
			return Character.toLowerCase( methodName.charAt( prefixLength ) ) + methodName.substring( prefixLength + 1 );
		else
			return methodName.substring( prefixLength ).toLowerCase();
	}
}

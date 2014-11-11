package com.knockchat.utils.meta.reflection;

import java.util.HashMap;
import java.util.Map;

import com.knockchat.utils.meta.MetaClass;
import com.knockchat.utils.meta.MetaClassFactory;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class ReflectionMetaClassFactory implements MetaClassFactory {

	private final Map<Class<?>, MetaClass> knownMetaclasses = new HashMap<Class<?>, MetaClass>();

	@Override
	public MetaClass get( Class<?> objectClass ) {
		MetaClass metaClass = knownMetaclasses.get( objectClass ); // Ищем метакласс в таблице известных

		if ( metaClass == null ) {
			metaClass = new ReflectionMetaClass( objectClass ); // Создаем новый экземпляр метакласса
			knownMetaclasses.put( objectClass, metaClass );
		}

		return metaClass;
	}

}

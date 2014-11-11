package com.knockchat.utils.meta.asm;

import java.util.HashMap;
import java.util.Map;

import com.knockchat.utils.meta.MetaClass;
import com.knockchat.utils.meta.MetaClassFactory;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class AsmMetaClassFactory implements MetaClassFactory {

	private final Map<Class<?>, MetaClass> knownMetaclasses = new HashMap<Class<?>, MetaClass>();

	private final DefiningClassLoader classLoader;

	public AsmMetaClassFactory( ClassLoader parent ) {
		classLoader = new DefiningClassLoader( parent );
	}

	@Override
	public MetaClass get( Class<?> objectClass ) {
		MetaClass metaClass = knownMetaclasses.get( objectClass ); // Ищем метакласс в таблице известных

		if ( metaClass == null ) {
			metaClass = new AsmMetaClass( objectClass, classLoader ); // Создаем новый экземпляр метакласса
			knownMetaclasses.put( objectClass, metaClass );
		}

		return metaClass;
	}
}

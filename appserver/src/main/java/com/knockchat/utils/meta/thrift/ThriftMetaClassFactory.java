package com.knockchat.utils.meta.thrift;

import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.knockchat.utils.meta.MetaClass;
import com.knockchat.utils.meta.MetaClassFactory;
import com.knockchat.utils.meta.asm.DefiningClassLoader;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class ThriftMetaClassFactory implements MetaClassFactory {
	
	private static final Logger log = LoggerFactory.getLogger(ThriftMetaClassFactory.class);

	private final Map<Class<?>, MetaClass> knownMetaclasses = new HashMap<Class<?>, MetaClass>();

	private final DefiningClassLoader classLoader;

	public ThriftMetaClassFactory( ClassLoader parent ) {
		classLoader = new DefiningClassLoader( parent );
	}

	@Override
	public MetaClass get( Class<?> objectClass ) {

		if (!TBase.class.isAssignableFrom(objectClass))
			return null;
		
		MetaClass metaClass = knownMetaclasses.get( objectClass ); // Ищем метакласс в таблице известных

		if ( metaClass == null ) {
			metaClass = new ThriftMetaClass( objectClass, classLoader ); // Создаем новый экземпляр метакласса
			knownMetaclasses.put( objectClass, metaClass );
		}

		return metaClass;
	}
}

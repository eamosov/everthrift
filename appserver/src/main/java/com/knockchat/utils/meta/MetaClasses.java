package com.knockchat.utils.meta;

import java.util.ArrayList;
import java.util.List;

import com.knockchat.utils.meta.asm.AsmMetaClassFactory;
import com.knockchat.utils.meta.thrift.ThriftMetaClassFactory;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class MetaClasses {

	private static final List<MetaClassFactory> factories = new ArrayList<MetaClassFactory>();
	
	static {
        MetaClasses.registerFactory(new ThriftMetaClassFactory(MetaClasses.class.getClassLoader())); // Фабрика метаклассов на базе Asm
        MetaClasses.registerFactory(new AsmMetaClassFactory(MetaClasses.class.getClassLoader())); // Фабрика метаклассов на базе Asm		
	}
	
	public static MetaClass get( Class<?> objectClass ) {
		for ( MetaClassFactory factory : factories ) {
			MetaClass mc = factory.get( objectClass );
			
			if ( mc != null )
				return mc;
		}
		
		return null;
	}
	
	public static void registerFactory( MetaClassFactory factory ) {
		if (!factories.contains(factory))
			factories.add( factory );
	}
	
	public static void unregisterFactory( MetaClassFactory factory ) {
		factories.remove( factory );
	}
	
	public static void unregisterAllFactories() {
		factories.clear();
	}
}

package com.knockchat.sql.mapper.mcb;

import java.util.regex.Pattern;

import com.knockchat.sql.mapper.AbstractSqlMapper;
import com.knockchat.utils.meta.MetaClass;
import com.knockchat.utils.meta.MetaClasses;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public abstract class AbstractMcbSqlMapper<ObjectType> extends AbstractSqlMapper<ObjectType> {	

	private static final Pattern nonNameSymbolPattern = Pattern.compile( "[^a-zA-Z0-9$]" );

	protected final MetaClass metaClass;

	public AbstractMcbSqlMapper( Class<ObjectType> objectClass ) {
		super( objectClass );		

		this.metaClass = MetaClasses.get( objectClass );
	}
	
	protected static String processName( String name ) {
		String res = nonNameSymbolPattern.matcher( name ).replaceAll( "" ).toUpperCase();
		return res;
	}
	
	protected String[] processNames( String[] names ) {
		String[] res = new String[ names.length];
		
		for ( int i = 0; i < names.length; ++ i )
			res[i] = processName( names[i] );
		
		return res;
	}
		
}
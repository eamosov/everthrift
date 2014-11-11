package com.knockchat.sql.mapper.mcb;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.knockchat.sql.SqlUtils;
import com.knockchat.utils.meta.MetaClass;
import com.knockchat.utils.meta.MetaClasses;
import com.knockchat.utils.meta.MetaProperty;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class McbFixedOrderSqlMapper<ObjectType>{
	
	private static final Pattern nonNameSymbolPattern = Pattern.compile( "[^a-zA-Z0-9$]" );
	
	private final MetaProperty[] metaProperties;
	
	private final MetaClass metaClass;
	
	private String processName( String name ) {
		String res = nonNameSymbolPattern.matcher( name ).replaceAll( "" ).toUpperCase();
		return res;
	}
	
	private String[] processNames( String[] names ) {
		String[] res = new String[ names.length];
		
		for ( int i = 0; i < names.length; ++ i )
			res[i] = processName( names[i] );
		
		return res;
	}	

	public McbFixedOrderSqlMapper( Class<ObjectType> objectClass, String... properties ) {
		this.metaClass = MetaClasses.get( objectClass );

		String[] processedProperties = processNames( properties );

		this.metaProperties = new MetaProperty[ properties.length ];

		for ( MetaProperty prop : metaClass.getProperties() ) {
			String pName = processName( prop.getName() );

			for ( int i = 0; i < properties.length; ++ i)
				if ( pName.equals( processedProperties[i] ))
					metaProperties[i] = prop;
		}
		
		// А теперь проверяем, все ли поля найдены		
		StringBuilder notFound = null;
		
		for ( int i = 0; i < properties.length; ++ i ) {
			if ( metaProperties[i] != null ) continue;
			
			if ( notFound == null ) {
				notFound = new StringBuilder();
				notFound.append( "'" );
				notFound.append( properties[i] );
				notFound.append( '\'' );
			} else {
				notFound.append( ",'" );
				notFound.append( properties[i] );
				notFound.append( '\'' );
			}				
		}
		
		if ( notFound != null )
			throw new RuntimeException( "Properties " + notFound + " not found in class " + objectClass.getName() );
	}

	public void fillParams( PreparedStatement st, ObjectType object ) throws SQLException {
		for ( int i = 0; i < metaProperties.length; ++ i )
			st.setObject( i + 1, SqlUtils.toSqlParam( metaProperties[i].get( object ) ) );
	}

	public List<Object> getParams(ObjectType object) throws SQLException {
		ArrayList<Object> result = new ArrayList<Object>();

		for ( int i = 0; i < metaProperties.length; ++ i )
			result.add(  metaProperties[i].get( object ) );

		return result;
	}
}

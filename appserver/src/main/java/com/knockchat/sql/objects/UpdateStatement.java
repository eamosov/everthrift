package com.knockchat.sql.objects;

import java.sql.SQLException;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public interface UpdateStatement<ObjectType> extends SqlStatement {

	int update(ObjectType obj, Object... values );
	int update(Throwable trace, ObjectType obj, Object... values ) throws SQLException;
	
}

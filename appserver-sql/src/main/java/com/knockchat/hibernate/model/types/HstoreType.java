package com.knockchat.hibernate.model.types;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.usertype.UserType;

import com.knockchat.utils.SqlUtils;

@SuppressWarnings({"rawtypes","unchecked"})
public class HstoreType implements UserType {

	@Override
	public int[] sqlTypes() {		
		return new int[]{Types.OTHER};
	}

	@Override
	public Class returnedClass() {
		return Map.class;
	}

	@Override
	public boolean equals(Object x, Object y) throws HibernateException {
		
		if (x==null && y == null)
			return true;
		
		if ((x == null && y!=null) || (x!=null && y==null))
			return false;
		
		return ((Map)x).equals(y);
	}

	@Override
	public int hashCode(Object x) throws HibernateException {
		
		if (x == null || ((Map)x).size() == 0)
			return 0;
		
		return ((Map)x).hashCode();
	}

	@Override
	public Object nullSafeGet(ResultSet rs, String[] names, SessionImplementor session, Object owner) throws HibernateException, SQLException {
		
		final Map<String,String> hstore = (Map<String,String>)rs.getObject(names[0]);
		
		if (hstore == null)
			return null;
		
		if (hstore.isEmpty())
			return new HashMap();
				
		return new HashMap(hstore);
	}

	@Override
	public void nullSafeSet(PreparedStatement st, Object value, int index, SessionImplementor session) throws HibernateException, SQLException {
		st.setString(index, value==null?null:(String)SqlUtils.toSqlParam((Map)value));
	}

	@Override
	public Object deepCopy(Object value) throws HibernateException {
		return value==null ? null :new HashMap((Map)value);
	}

	@Override
	public boolean isMutable() {
		return true;
	}

    @Override
    public Object assemble(final Serializable cached, final Object owner) throws HibernateException {
        return deepCopy(cached);
    }


    @Override
    public Serializable disassemble(final Object o) throws HibernateException {
        return (Serializable) deepCopy(o);
    }

	@Override
	public Object replace(Object original, Object target, Object owner) throws HibernateException {
		return original == null ? null: deepCopy(original);
	}

}

package com.knockchat.hibernate.model.types;

import gnu.trove.impl.hash.THash;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.usertype.UserType;

import com.knockchat.sql.SqlUtils;

@SuppressWarnings({"rawtypes","unchecked"})
public abstract class Trove4jHstoreType<T extends THash> implements UserType {

	@Override
	public int[] sqlTypes() {		
		return new int[]{Types.OTHER};
	}

	@Override
	public boolean equals(Object x, Object y) throws HibernateException {
		
		if (x==null && y == null)
			return true;
		
		if ((x == null && y!=null) || (x!=null && y==null))
			return false;
		
		return x.equals(y);
	}

	@Override
	public int hashCode(Object x) throws HibernateException {
		
		if (x == null || ((T)x).size() == 0)
			return 0;
		
		return x.hashCode();
	}
	
	protected abstract T transform(Map<String,String> input);
	protected abstract Map transformReverse(T input);

	@Override
	public Object nullSafeGet(ResultSet rs, String[] names, SessionImplementor session, Object owner) throws HibernateException, SQLException {
		
		final Map<String,String> hstore = (Map<String,String>)rs.getObject(names[0]);
		
		if (hstore == null)
			return null;
		
		return transform(hstore);				
	}

	@Override
	public void nullSafeSet(PreparedStatement st, Object value, int index, SessionImplementor session) throws HibernateException, SQLException {
		st.setString(index, value==null?null:(String)SqlUtils.toSqlParam(transformReverse((T)value)));
	}

//	@Override
//	public Object deepCopy(Object value) throws HibernateException {
//		return value==null?new HashMap<>():new HashMap((Map)value);
//	}

	@Override
	public boolean isMutable() {
		return true;
	}

    @Override
    public Object assemble(final Serializable cached, final Object owner) throws HibernateException {
        return cached;
    }


    @Override
    public Serializable disassemble(final Object o) throws HibernateException {
        return (Serializable) o;
    }

	@Override
	public Object replace(Object original, Object target, Object owner) throws HibernateException {
		return original == null ? null: deepCopy(original);
	}

}

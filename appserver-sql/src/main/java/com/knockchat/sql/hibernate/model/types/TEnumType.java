package com.knockchat.sql.hibernate.model.types;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.thrift.TEnum;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.usertype.UserType;

public abstract class TEnumType<T extends TEnum> implements UserType {
	
	protected abstract Class<T> getTEnumClass();
	
	private final Method findByValue;
	
	public TEnumType(){
		try {
			findByValue = getTEnumClass().getMethod("findByValue", Integer.TYPE);
		} catch (NoSuchMethodException | SecurityException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int[] sqlTypes() {
		return new int[]{Types.INTEGER};
	}

	@Override
	public Class returnedClass() {
		return getTEnumClass();
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
		if (x == null)
			return 0;
		
		return x.hashCode();
	}

	@Override
	public Object nullSafeGet(ResultSet rs, String[] names, SessionImplementor session, Object owner) throws HibernateException, SQLException {
				
		final Integer value  = (Integer)rs.getObject(names[0]);
		
		if (value == null)
			return null;

		try {
			return findByValue.invoke(null, value.intValue());
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public void nullSafeSet(PreparedStatement st, Object value, int index, SessionImplementor session) throws HibernateException, SQLException {
		if (value == null)
			st.setNull(index, java.sql.Types.INTEGER);
		else
			st.setInt(index, ((TEnum)value).getValue());
	}

	@Override
	public Object deepCopy(Object value) throws HibernateException {
		return value;
	}

	@Override
	public boolean isMutable() {
		return false;
	}

	@Override
	public Serializable disassemble(Object value) throws HibernateException {
		return (Serializable) value;
	}

	@Override
	public Object assemble(Serializable cached, Object owner) throws HibernateException {
		return cached;
	}

	@Override
	public Object replace(Object original, Object target, Object owner) throws HibernateException {
		return original;
	}

}

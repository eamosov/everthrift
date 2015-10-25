package com.knockchat.hibernate.model.types;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.usertype.UserType;
import org.postgresql.util.PGobject;

public class LongIntervalType implements UserType {

	public LongIntervalType() {
		
	}

	@Override
	public int[] sqlTypes() {
		return new int[]{Types.OTHER};
	}

	@Override
	public Class returnedClass() {
		return Long.class;
	}

	@Override
	public boolean equals(Object x, Object y) throws HibernateException {
		if (x == null && y == null)
			return true;

		if ((x == null && y != null) || (x != null && y == null))
			return false;

		return x.equals(y);
	}

	@Override
	public int hashCode(Object x) throws HibernateException {		
		return x.hashCode();
	}

	@Override
	public Object nullSafeGet(ResultSet rs, String[] names, SessionImplementor session, Object owner) throws HibernateException, SQLException {		
		return rs.getLong(names[0]) * 1000;
	}

	@Override
	public void nullSafeSet(PreparedStatement st, Object value, int index, SessionImplementor session) throws HibernateException, SQLException {
		
		final PGobject o = new PGobject();
		o.setType("interval");
		o.setValue(value == null ? null : ((Long) value / 1000) + " seconds");
		st.setObject(index, o);
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
		return (Serializable)value;
	}

	@Override
	public Object assemble(Serializable cached, Object owner) throws HibernateException {
		return cached;
	}

	@Override
	public Object replace(Object original, Object target, Object owner)throws HibernateException {
		return original;
	}

}

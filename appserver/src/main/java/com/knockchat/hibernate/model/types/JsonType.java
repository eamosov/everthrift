package com.knockchat.hibernate.model.types;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Objects;

import org.apache.thrift.TBase;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.usertype.UserType;
import org.postgresql.util.PGobject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.knockchat.utils.GsonSerializer.TBaseSerializer;

@SuppressWarnings({"unchecked"})
public abstract class JsonType implements UserType {
		
	private final Gson gson = new GsonBuilder().registerTypeHierarchyAdapter(TBase.class, new TBaseSerializer()).create();

	@Override
	public int[] sqlTypes() {		
		return new int[]{Types.OTHER};
	}

	@Override
	public boolean equals(Object x, Object y) throws HibernateException {		
		return Objects.equals(x, y);
	}

	@Override
	public int hashCode(Object x) throws HibernateException {
		
		if (x == null)
			return 0;
		
		return x.hashCode();
	}

	@Override
	public Object nullSafeGet(ResultSet rs, String[] names, SessionImplementor session, Object owner) throws HibernateException, SQLException {
		
		final PGobject json = (PGobject)rs.getObject(names[0]);
		
		if (json == null)
			return null;
		
		return gson.fromJson(json.getValue(), this.returnedClass());
	}

	@Override
	public void nullSafeSet(PreparedStatement st, Object value, int index, SessionImplementor session) throws HibernateException, SQLException {
		
		final PGobject json = new PGobject();
		json.setType("jsonb");
		json.setValue(value == null ? null : gson.toJson(value));		
		st.setObject(index, json);
	}

	@Override
	public Object deepCopy(Object value) throws HibernateException {
		try {
			return value==null ? null : value.getClass().getConstructor(value.getClass()).newInstance(value);
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			throw new HibernateException(e);
		}
	}

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
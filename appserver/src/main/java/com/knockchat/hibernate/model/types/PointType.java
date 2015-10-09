package com.knockchat.hibernate.model.types;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.usertype.UserType;

import com.knockchat.utils.meta.MetaClasses;
import com.knockchat.utils.meta.MetaProperty;

@SuppressWarnings("rawtypes")
public abstract class PointType implements UserType {

	final Constructor create;
	final Constructor copy;

	private final MetaProperty x;
	private final MetaProperty y;
	
	private final static Pattern pointPattern = Pattern.compile("POINT\\(([0-9.-]+) ([0-9.-]+)\\)");

	@Override
	public int[] sqlTypes() {
		return new int[] { Types.OTHER };
	}

	@SuppressWarnings("unchecked")
	public PointType() throws NoSuchMethodException, SecurityException {

		create = returnedClass().getConstructor();
		copy = returnedClass().getConstructor(returnedClass());

		x = MetaClasses.get(returnedClass()).getProperty("x");
		y = MetaClasses.get(returnedClass()).getProperty("y");
	}

	public static boolean isCompatible(final Class cls) {

		try {
			final PointType d = new PointType() {

				@Override
				public Class returnedClass() {
					return cls;
				}
			};

			final Object o = cls.newInstance();
			d.x.set(o, 0.0);
			d.y.set(o, 0.0);
			return true;
		} catch (Exception e) {
			return false;
		}
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
		if (x == null)
			return 0;

		return x.hashCode();
	}

	@Override
	public Object nullSafeGet(ResultSet rs, String[] names,
			SessionImplementor session, Object owner)
			throws HibernateException, SQLException {

		final Object value = rs.getObject(names[0]);

		if (value == null)
			return null;
		
		final Matcher m = pointPattern.matcher(value.toString());
		if (!m.matches())
			throw new HibernateException("invalid POINT presentation:"
					+ value.toString());

		Object ret;
		try {
			ret = create.newInstance();
			x.set(ret, Double.parseDouble(m.group(1)));
			y.set(ret, Double.parseDouble(m.group(2)));
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			throw new SQLException(e);
		}

		return ret;
	}

	@Override
	public void nullSafeSet(PreparedStatement st, Object value, int index,
			SessionImplementor session) throws HibernateException, SQLException {

		if (value == null) {
			st.setNull(index, java.sql.Types.OTHER);
			return;
		}

		final Double _x = (Double)x.get(value);
		final Double _y = (Double)y.get(value);

		st.setString(index, String.format(Locale.ENGLISH, "SRID=4326;POINT(%f %f)", _x, _y));
	}

	@Override
	public Object deepCopy(Object value) throws HibernateException {

		if (value == null)
			return null;

		try {
			return copy.newInstance(value);
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			return new HibernateException(e);
		}
	}

	@Override
	public boolean isMutable() {
		return true;
	}

	@Override
	public Serializable disassemble(Object value) throws HibernateException {
		return (Serializable) deepCopy(value);
	}

	@Override
	public Object assemble(Serializable cached, Object owner) throws HibernateException {
		return deepCopy(cached);
	}

	@Override
	public Object replace(Object original, Object target, Object owner)
			throws HibernateException {
		return original == null ? null : deepCopy(original);
	}

}

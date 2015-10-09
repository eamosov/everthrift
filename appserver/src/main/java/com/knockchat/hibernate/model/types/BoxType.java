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
import org.postgresql.util.PGobject;

import com.knockchat.utils.meta.MetaClass;
import com.knockchat.utils.meta.MetaClasses;
import com.knockchat.utils.meta.MetaProperty;

@SuppressWarnings("rawtypes")
public abstract class BoxType implements UserType {

	//private static final Logger log = LoggerFactory.getLogger(BoxType.class);

	final Constructor create;
	final Constructor copy;

	private final MetaProperty min;
	private final MetaClass minType;
	private final MetaProperty minX;
	private final MetaProperty minY;

	private final MetaProperty max;
	private final MetaClass maxType;
	private final MetaProperty maxX;
	private final MetaProperty maxY;

	private final static Pattern boxPattern = Pattern.compile("BOX\\(([0-9.-]+) ([0-9.-]+),([0-9.-]+) ([0-9.-]+)\\)");

	@Override
	public int[] sqlTypes() {
		return new int[] { Types.OTHER };
	}

	@SuppressWarnings("unchecked")
	public BoxType() throws NoSuchMethodException, SecurityException {

		create = returnedClass().getConstructor();
		copy = returnedClass().getConstructor(returnedClass());

		min = MetaClasses.get(returnedClass()).getProperty("min");
		minType = MetaClasses.get(min.getType());
		minX = minType.getProperty("x");
		minY = minType.getProperty("y");

		max = MetaClasses.get(returnedClass()).getProperty("max");
		maxType = MetaClasses.get(max.getType());
		maxX = maxType.getProperty("x");
		maxY = maxType.getProperty("y");

	}

	public static boolean isCompatible(final Class cls) {

		try {
			final BoxType d = new BoxType() {

				@Override
				public Class returnedClass() {
					return cls;
				}
			};

			final Object o = cls.newInstance();

			final Object min = d.minType.newInstance();
			d.minX.set(min, 0.0);
			d.minY.set(min, 0.0);

			final Object max = d.maxType.newInstance();
			d.maxX.set(max, 0.0);
			d.maxY.set(max, 0.0);

			d.min.set(o, min);
			d.max.set(o, max);
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

		final Matcher m = boxPattern.matcher(value.toString());
		if (!m.matches())
			throw new HibernateException("invalid box2d presentation:"
					+ value.toString());

		Object ret;
		try {
			ret = create.newInstance();

			final Object _min = minType.newInstance();
			minX.set(_min, Double.parseDouble(m.group(1)));
			minY.set(_min, Double.parseDouble(m.group(2)));

			final Object _max = maxType.newInstance();
			maxX.set(_max, Double.parseDouble(m.group(3)));
			maxY.set(_max, Double.parseDouble(m.group(4)));

			min.set(ret, _min);
			max.set(ret, _max);

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

		final Object _min = min.get(value);
		final Object _max = max.get(value);

		final Double _minX = (Double) minX.get(_min);
		final Double _minY = (Double) minY.get(_min);
		final Double _maxX = (Double) maxX.get(_max);
		final Double _maxY = (Double) maxY.get(_max);

		final PGobject o = new PGobject();
		o.setType("box2d");
		o.setValue(String.format(Locale.ENGLISH, "BOX(%f %f,%f %f)", _minX,
				_minY, _maxX, _maxY));
		st.setObject(index, o);
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

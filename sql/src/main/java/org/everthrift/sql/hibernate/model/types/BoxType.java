package org.everthrift.sql.hibernate.model.types;

import java.beans.PropertyDescriptor;
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
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;
import org.postgresql.util.PGobject;
import org.springframework.beans.BeanUtils;

@SuppressWarnings("rawtypes")
public abstract class BoxType implements UserType {

    //private static final Logger log = LoggerFactory.getLogger(BoxType.class);

    final Constructor create;
    final Constructor copy;

    private final PropertyDescriptor min;
    private final Class minType;
    private final PropertyDescriptor minX;
    private final PropertyDescriptor minY;

    private final PropertyDescriptor max;
    private final Class maxType;
    private final PropertyDescriptor maxX;
    private final PropertyDescriptor maxY;

    private final static Pattern boxPattern = Pattern.compile("BOX\\(([0-9.-]+) ([0-9.-]+),([0-9.-]+) ([0-9.-]+)\\)");

    @Override
    public int[] sqlTypes() {
        return new int[] { Types.OTHER };
    }

    @SuppressWarnings("unchecked")
    public BoxType() throws NoSuchMethodException, SecurityException {

        create = returnedClass().getConstructor();
        copy = returnedClass().getConstructor(returnedClass());

        min = BeanUtils.getPropertyDescriptor(returnedClass(), "min");
        minType = min.getPropertyType();
        minX = BeanUtils.getPropertyDescriptor(minType, "x");
        minY = BeanUtils.getPropertyDescriptor(minType, "y");

        max = BeanUtils.getPropertyDescriptor(returnedClass(), "max");
        maxType = max.getPropertyType();
        maxX = BeanUtils.getPropertyDescriptor(maxType, "x");
        maxY = BeanUtils.getPropertyDescriptor(maxType, "y");

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
            d.minX.getWriteMethod().invoke(min, 0.0);
            d.minY.getWriteMethod().invoke(min, 0.0);

            final Object max = d.maxType.newInstance();
            d.maxX.getWriteMethod().invoke(max, 0.0);
            d.maxY.getWriteMethod().invoke(max, 0.0);

            d.min.getWriteMethod().invoke(o, min);
            d.max.getWriteMethod().invoke(o, max);
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
            SharedSessionContractImplementor session, Object owner)
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
            minX.getWriteMethod().invoke(_min, Double.parseDouble(m.group(1)));
            minY.getWriteMethod().invoke(_min, Double.parseDouble(m.group(2)));

            final Object _max = maxType.newInstance();
            maxX.getWriteMethod().invoke(_max, Double.parseDouble(m.group(3)));
            maxY.getWriteMethod().invoke(_max, Double.parseDouble(m.group(4)));

            min.getWriteMethod().invoke(ret, _min);
            max.getWriteMethod().invoke(ret, _max);

        } catch (InstantiationException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException e) {
            throw new SQLException(e);
        }

        return ret;
    }

    @Override
    public void nullSafeSet(PreparedStatement st, Object value, int index,
            SharedSessionContractImplementor session) throws HibernateException, SQLException {

        if (value == null) {
            st.setNull(index, java.sql.Types.OTHER);
            return;
        }

        ;
        try {
            final Object _min = min.getReadMethod().invoke(value);
            final Object _max = max.getReadMethod().invoke(value);

            final Double _minX = (Double) minX.getReadMethod().invoke(_min);
            final Double _minY = (Double) minY.getReadMethod().invoke(_min);
            final Double _maxX = (Double) maxX.getReadMethod().invoke(_max);
            final Double _maxY = (Double) maxY.getReadMethod().invoke(_max);

            final PGobject o = new PGobject();
            o.setType("box2d");
            o.setValue(String.format(Locale.ENGLISH, "BOX(%f %f,%f %f)", _minX,
                    _minY, _maxX, _maxY));
            st.setObject(index, o);

        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new HibernateException(e);
        }
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

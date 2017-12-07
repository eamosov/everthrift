package org.everthrift.sql.hibernate.model.types;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.BeanUtils;

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

@SuppressWarnings("rawtypes")
public abstract class PointType implements UserType {

    final Constructor create;

    final Constructor copy;

    private final PropertyDescriptor x;

    private final PropertyDescriptor y;

    private final static Pattern pointPattern = Pattern.compile("POINT\\(([0-9.-]+) ([0-9.-]+)\\)");

    @NotNull
    @Override
    public int[] sqlTypes() {
        return new int[]{Types.OTHER};
    }

    @SuppressWarnings("unchecked")
    public PointType() throws NoSuchMethodException, SecurityException {

        create = returnedClass().getConstructor();
        copy = returnedClass().getConstructor(returnedClass());

        x = BeanUtils.getPropertyDescriptor(returnedClass(), "x");
        y = BeanUtils.getPropertyDescriptor(returnedClass(), "y");
    }

    public static boolean isCompatible(@NotNull final Class cls) {

        try {
            final PointType d = new PointType() {

                @NotNull
                @Override
                public Class returnedClass() {
                    return cls;
                }
            };

            final Object o = cls.newInstance();
            d.x.getWriteMethod().invoke(o, 0.0);
            d.y.getWriteMethod().invoke(o, 0.0);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean equals(@Nullable Object x, @Nullable Object y) throws HibernateException {
        if (x == null && y == null) {
            return true;
        }

        if ((x == null && y != null) || (x != null && y == null)) {
            return false;
        }

        return x.equals(y);
    }

    @Override
    public int hashCode(@Nullable Object x) throws HibernateException {
        if (x == null) {
            return 0;
        }

        return x.hashCode();
    }

    @Nullable
    @Override
    public Object nullSafeGet(@NotNull ResultSet rs, String[] names, SharedSessionContractImplementor session,
                              Object owner) throws HibernateException, SQLException {

        final Object value = rs.getObject(names[0]);

        if (value == null) {
            return null;
        }

        final Matcher m = pointPattern.matcher(value.toString());
        if (!m.matches()) {
            throw new HibernateException("invalid POINT presentation:" + value.toString());
        }

        Object ret;
        try {
            ret = create.newInstance();
            x.getWriteMethod().invoke(ret, Double.parseDouble(m.group(1)));
            y.getWriteMethod().invoke(ret, Double.parseDouble(m.group(2)));
        } catch (@NotNull InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new SQLException(e);
        }

        return ret;
    }

    @Override
    public void nullSafeSet(@NotNull PreparedStatement st, @Nullable Object value, int index,
                            SharedSessionContractImplementor session) throws HibernateException, SQLException {

        if (value == null) {
            st.setNull(index, java.sql.Types.OTHER);
            return;
        }

        try {
            final Double _x = (Double) x.getReadMethod().invoke(value);
            final Double _y = (Double) y.getReadMethod().invoke(value);

            st.setString(index, String.format(Locale.ENGLISH, "SRID=4326;POINT(%f %f)", _x, _y));
        } catch (@NotNull IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new HibernateException(e);
        }
    }

    @Nullable
    @Override
    public Object deepCopy(@Nullable Object value) throws HibernateException {

        if (value == null) {
            return null;
        }

        try {
            return copy.newInstance(value);
        } catch (@NotNull InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            return new HibernateException(e);
        }
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Nullable
    @Override
    public Serializable disassemble(Object value) throws HibernateException {
        return (Serializable) deepCopy(value);
    }

    @Nullable
    @Override
    public Object assemble(Serializable cached, Object owner) throws HibernateException {
        return deepCopy(cached);
    }

    @Nullable
    @Override
    public Object replace(@Nullable Object original, Object target, Object owner) throws HibernateException {
        return original == null ? null : deepCopy(original);
    }

}

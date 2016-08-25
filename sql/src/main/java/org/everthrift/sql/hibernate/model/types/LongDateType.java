package org.everthrift.sql.hibernate.model.types;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.everthrift.utils.LongTimestamp;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;

public class LongDateType implements UserType {

    public LongDateType() {

    }

    @Override
    public int[] sqlTypes() {
        return new int[] { Types.DATE };
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
    public Object nullSafeGet(ResultSet rs, String[] names, SharedSessionContractImplementor session,
                              Object owner) throws HibernateException, SQLException {

        final java.sql.Date value = (java.sql.Date) rs.getObject(names[0]);

        if (value == null)
            return null;

        return LongTimestamp.from(value);
    }

    @Override
    public void nullSafeSet(PreparedStatement st, Object value, int index,
                            SharedSessionContractImplementor session) throws HibernateException, SQLException {

        if (value == null)
            st.setNull(index, Types.DATE);
        else
            st.setDate(index, new java.sql.Date(LongTimestamp.toMillis((Long) value)));
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

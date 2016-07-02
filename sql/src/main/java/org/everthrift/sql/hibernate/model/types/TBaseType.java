package org.everthrift.sql.hibernate.model.types;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.everthrift.thrift.TBaseLazyModel;
import org.everthrift.thrift.TBaseModel;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

public abstract class TBaseType implements UserType {

    private static final Logger log = LoggerFactory.getLogger(TBaseType.class);

    final Constructor<TBaseModel> init;

    public TBaseType(){

        try {
            init = returnedClass().getConstructor();
        } catch (NoSuchMethodException | SecurityException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public int[] sqlTypes() {
        return new int[]{Types.BINARY};
    }

    @Override
    public abstract Class returnedClass();

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
    public Object nullSafeGet(ResultSet rs, String[] names, SharedSessionContractImplementor session, Object owner) throws HibernateException, SQLException {

        final byte[] bytes = rs.getBytes(names[0]);

        if (log.isDebugEnabled())
            log.debug("Load {} bytes for type {}", bytes == null ? 0 : bytes.length, returnedClass().getSimpleName());

        if (bytes == null)
            return null;

        try {
            final TBaseModel o = init.newInstance();
            o.read(bytes, 0);
            return o;
        } catch (Exception e) {
            if (e instanceof RuntimeException)
                throw (RuntimeException)e;
            else
                throw new HibernateException(e);
        }
    }

    @Override
    public void nullSafeSet(PreparedStatement st, Object value, int index, SharedSessionContractImplementor session) throws HibernateException, SQLException {

        if (value == null){
            st.setNull(index, Types.BINARY);
        }else{
            st.setBytes(index, ((TBaseModel)value).write());
        }
    }

    @Override
    public Object deepCopy(Object value) throws HibernateException {
        return value==null ? null : ((TBaseModel)value).deepCopy();
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public Serializable disassemble(Object value) throws HibernateException {

        if (value == null)
            return null;

        return (value instanceof TBaseLazyModel) ? ((TBaseLazyModel)value).write() : (Serializable)deepCopy(value);
    }

    @Override
    public Object assemble(Serializable cached, Object owner) throws HibernateException {

        if (cached == null)
            return null;

        if (cached instanceof byte[]){
            try {
                final TBaseModel o = init.newInstance();
                o.read((byte[])cached, 0);
                return o;
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new HibernateException(e);
            }
        }else{
            return deepCopy(cached);
        }
    }

    @Override
    public Object replace(Object original, Object target, Object owner)throws HibernateException {
        return original == null ? null: deepCopy(original);
    }

}

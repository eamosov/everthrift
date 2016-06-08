package com.knockchat.sql.hibernate.model.types;

import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.usertype.UserType;
import org.springframework.beans.BeanUtils;

@SuppressWarnings("rawtypes")
public abstract class DateType implements UserType {
		
	@Override
	public abstract Class returnedClass();
	
	private final PropertyDescriptor year; //the year
	private final PropertyDescriptor month;  //the month between 0-11
	private final PropertyDescriptor date;	//the day of the month between 1-31
	
	private Constructor copy;
	
	@SuppressWarnings("unchecked")
	public DateType(){		
		year = BeanUtils.getPropertyDescriptor(returnedClass(), "year");
		month = BeanUtils.getPropertyDescriptor(returnedClass(), "month");
		date = BeanUtils.getPropertyDescriptor(returnedClass(), "date");
		
		try {
			copy = returnedClass().getConstructor(returnedClass());
		} catch (NoSuchMethodException | SecurityException e) {
			throw new RuntimeException(e);
		}
		
		if (year == null || month == null || date ==null)
			throw new RuntimeException("coudn't found properties year/month/date");
	}
	
	public static boolean isCompatible(final Class cls){
		
		try{
			final DateType d = new DateType(){

				@Override
				public Class returnedClass() {
					return cls;
				}			
			};
			
			final Object o = cls.newInstance();
			
			d.year.getWriteMethod().invoke(o, 0);
			d.month.getWriteMethod().invoke(o, 0);
			d.date.getWriteMethod().invoke(o, 0);		

			return true;
		}catch(Exception e){
			return false;
		}
	}

	@Override
	public int[] sqlTypes() {
		return new int[]{Types.DATE};
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
				
		final Date value  = rs.getDate(names[0]);
		
		if (value == null)
			return null;

		Object ret;
		try {
			ret = returnedClass().newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new SQLException(e);
		}
		
		final Calendar cld = Calendar.getInstance();
		cld.setTime(value);
		
		try {
			year.getWriteMethod().invoke(ret, cld.get(Calendar.YEAR));
			month.getWriteMethod().invoke(ret, cld.get(Calendar.MONTH)); /*0-11*/
			date.getWriteMethod().invoke(ret, cld.get(Calendar.DATE)); /*1-31*/			
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new HibernateException(e);
		}
		
		return ret;
	}

	@Override
	public void nullSafeSet(PreparedStatement st, Object value, int index, SessionImplementor session) throws HibernateException, SQLException {
		
		if (value == null){
			st.setNull(index, java.sql.Types.DATE);
			return;
		}
		
		final Calendar cld = Calendar.getInstance();
		
		
		try {
			Number y = (Number)year.getReadMethod().invoke(value);
			
			if (y!=null)
				cld.set(Calendar.YEAR, y.intValue());

			final Number m =  (Number)month.getReadMethod().invoke(value);
			if (m!=null)
				cld.set(Calendar.MONTH, m.intValue());
			
			final Number d =  (Number)date.getReadMethod().invoke(value);
			if (d!=null)
				cld.set(Calendar.DATE, d.intValue());
							
			final Date date = new Date(cld.getTimeInMillis());
			st.setDate(index, date);
			
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
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new HibernateException(e);
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
	public Object replace(Object original, Object target, Object owner) throws HibernateException {
		return original == null ? null: deepCopy(original);
	}

}

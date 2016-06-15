package org.everthrift.cassandra.codecs;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.everthrift.appserver.utils.ClassUtils;

import com.datastax.driver.core.CodecUtils;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ParseUtils;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class DateCodec<T> extends TypeCodec<T> {
	
	private static class Factory<T> implements TypeCodecFactory<T> {

		@Override
		public boolean accepts(Class<?> javaType) {
			return getDateProps(javaType) !=null;
		}

		@Override
		public boolean accepts(DataType cqlType) {
			return cqlType.equals(DataType.timestamp());
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public TypeCodec<T> create(DataType cqlType, Class<?> javaType) {
			return (TypeCodec)new DateCodec((Class)javaType);
		}		
	}
	
	public static final TypeCodecFactory<?> factory = new Factory(); 
	

	private static final String pattern = "yyyy-MM-dd";
	
	final PropertyDescriptor year;
	final PropertyDescriptor month;
	final PropertyDescriptor date;

	protected DateCodec(Class<T> javaClass) {
		super(DataType.timestamp(), javaClass);
		
		final PropertyDescriptor props[] = getDateProps(javaClass);
		year = props[0];
		month = props[1];
		date = props[2];
	}
	
	private static final PropertyDescriptor[] getDateProps(Class javaClass){
		
		final Map<String, PropertyDescriptor> entityProps = ClassUtils.getPropertyDescriptors(javaClass); 

		final PropertyDescriptor year = entityProps.get("year");
		final PropertyDescriptor month = entityProps.get("month");
		final PropertyDescriptor date = entityProps.get("date");
		if (year !=null && month !=null && date !=null)
			return new PropertyDescriptor[]{year, month, date};
		else
			return null;
	}
	
	private Date toDate(T value) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException{
		final Calendar cld = Calendar.getInstance();
		
		final Number y =  (Number)year.getReadMethod().invoke(value);
		if (y!=null)
			cld.set(Calendar.YEAR, y.intValue());

		final Number m =  (Number)month.getReadMethod().invoke(value);
		if (m!=null)
			cld.set(Calendar.MONTH, m.intValue());
		
		final Number d =  (Number)date.getReadMethod().invoke(value);
		if (d!=null)
			cld.set(Calendar.DATE, d.intValue());
						
		return new Date(cld.getTimeInMillis());		
	}
	
	private T fromDate(Date value) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException{
		final T ret = (T)javaType.getRawType().newInstance();
		
		final Calendar cld = Calendar.getInstance();
		cld.setTime(value);
				
		year.getWriteMethod().invoke(ret, cld.get(Calendar.YEAR));
		month.getWriteMethod().invoke(ret, cld.get(Calendar.MONTH)); /*0-11*/
		date.getWriteMethod().invoke(ret, cld.get(Calendar.DATE)); /*1-31*/
		return ret;
	}

	@Override
	public ByteBuffer serialize(T value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null)
            return null;
        
        ByteBuffer bb = ByteBuffer.allocate(8);
        try {
			bb.putLong(0, toDate(value).getTime());
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new InvalidTypeException("Coudn't serialize Date", e);
		}
        return bb;
	}

	@Override
	public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
		
        if (bytes == null || bytes.remaining() == 0)
            return null;
        
        if (bytes.remaining() != 8)
            throw new InvalidTypeException("Invalid 64-bits long value, expecting 8 bytes but got " + bytes.remaining());

        try {
			return fromDate(new Date(bytes.getLong(bytes.position())));
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new InvalidTypeException("Coudn't deserialize Date", e);
		}
	}

	@Override
	public T parse(String value) throws InvalidTypeException {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;

        // single quotes are optional for long literals, mandatory for date patterns
        // strip enclosing single quotes, if any
        if (ParseUtils.isQuoted(value))
            value = ParseUtils.unquote(value);

        if (ParseUtils.isLongLiteral(value)) {
            long unsigned;
            try {
                unsigned = Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value), e);
            }
            try {
                return fromDate(new Date(TimeUnit.DAYS.toMillis(CodecUtils.fromCqlDateToDaysSinceEpoch(unsigned))));
            } catch (IllegalArgumentException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value), e);
            }
        }

        try {
            return fromDate(ParseUtils.parseDate(value, pattern));
        } catch (ParseException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value), e);
        }
	}

	@Override
	public String format(T value) throws InvalidTypeException {
        if (value == null)
            return "NULL";
        
        try{
        	final Number y =  (Number)year.getReadMethod().invoke(value);
        	final Number m =  (Number)month.getReadMethod().invoke(value);
        	final Number d =  (Number)date.getReadMethod().invoke(value);        
        	return String.format("%d-%s-%s", y, pad2(m.intValue()+1), pad2(d.intValue()));
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new InvalidTypeException("couldn't format", e);
		}
	}

    private static String pad2(int i) {
        String s = Integer.toString(i);
        return s.length() == 2 ? s : "0" + s;
    }

}

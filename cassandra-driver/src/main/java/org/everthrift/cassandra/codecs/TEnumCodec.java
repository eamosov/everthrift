package org.everthrift.cassandra.codecs;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.apache.thrift.TEnum;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class TEnumCodec<T extends TEnum> extends TypeCodec<T>{

    private static class Factory<T> implements TypeCodecFactory<T> {

        @Override
        public boolean accepts(Class<?> javaType) {
            return TEnum.class.isAssignableFrom(javaType);
        }

        @Override
        public boolean accepts(DataType cqlType) {
            return cqlType.equals(DataType.cint());
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public TypeCodec<T> create(DataType cqlType, Class<?> javaType) {
            return (TypeCodec)new TEnumCodec((Class)javaType);
        }
    }

    public static final TypeCodecFactory<?> factory = new Factory();

    private final Method findByValue;

    protected TEnumCodec(Class<T> javaClass) {
        super(DataType.cint(), javaClass);

        try {
            findByValue = javaClass.getMethod("findByValue", Integer.TYPE);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    private T byValue(int value) throws InvalidTypeException{
        try {
            return (T)findByValue.invoke(null, value);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new InvalidTypeException("coudn't convert " + value + " to " + javaType.toString(), e);
        }
    }

    @Override
    public ByteBuffer serialize(T value, ProtocolVersion protocolVersion) throws InvalidTypeException {

        if (value == null)
            return null;

        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(0, value.getValue());
        return bb;
    }

    @Override
    public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {

        if (bytes == null || bytes.remaining() == 0)
            return null;

        if (bytes.remaining() != 4)
            throw new InvalidTypeException("Invalid 32-bits integer value, expecting 4 bytes but got " + bytes.remaining());

        return byValue(bytes.getInt(bytes.position()));
    }

    @Override
    public T parse(String value) throws InvalidTypeException {

        try {
            return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL") ? null : byValue(Integer.parseInt(value));
        } catch (NumberFormatException e) {
            throw new InvalidTypeException(String.format("Cannot parse 32-bits int value from \"%s\"", value));
        }
    }

    @Override
    public String format(T value) throws InvalidTypeException {

        if (value == null)
            return "NULL";

        return Integer.toString(value.getValue());
    }
}

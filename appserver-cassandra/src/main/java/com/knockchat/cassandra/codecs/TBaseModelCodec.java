package com.knockchat.cassandra.codecs;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.reflect.TypeToken;
import com.knockchat.utils.thrift.TBaseHasModel;
import com.knockchat.utils.thrift.TBaseModel;

public class TBaseModelCodec<T extends TBaseModel<?,?>> extends TypeCodec<T> {
	
	private static class Factory<T extends TBaseModel<?,?>> implements TypeCodecFactory<T> {

		@Override
		public boolean accepts(Class<?> javaType) {
			return TBaseModel.class.isAssignableFrom(javaType) || (TBaseHasModel.getModel((Class)javaType) !=null);
		}

		@Override
		public boolean accepts(DataType cqlType) {
			return cqlType.equals(DataType.blob());
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public TypeCodec<T> create(DataType cqlType, Class<?> javaType) {
			return (TypeCodec)new TBaseModelCodec((Class)(TBaseModel.class.isAssignableFrom(javaType) ? javaType : TBaseHasModel.getModel((Class)javaType)));
		}		
	}
	
	public static final TypeCodecFactory<? extends TBaseModel<?,?>> factory = new Factory<TBaseModel<?,?>>(); 

	public TBaseModelCodec(Class<T> javaClass) {
		super(DataType.blob(), javaClass);
	}
	
	@Override
    public boolean accepts(TypeToken javaType) {
        checkNotNull(javaType, "Parameter javaType cannot be null");
        return javaType.getRawType().isAssignableFrom(this.javaType.getRawType());
    }
	

	@Override
	public ByteBuffer serialize(T value, ProtocolVersion protocolVersion) throws InvalidTypeException {
		return value == null ? null : ByteBuffer.wrap(value.write());
	}

	@Override
	public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
		
		if (bytes == null || bytes.remaining() == 0)
            return null;

		try {
			final T entity = (T)(((Class)javaType.getRawType()).newInstance());
			entity.read(bytes.array(), bytes.arrayOffset() + bytes.position());
			return entity;
		} catch (InstantiationException | IllegalAccessException e) {
			throw new InvalidTypeException("", e);
		}
	}

	@Override
	public T parse(String value) throws InvalidTypeException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String format(T value) throws InvalidTypeException {
		return value.toString();
	}

}

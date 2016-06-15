package org.everthrift.cassandra.codecs;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class StringUuidCodec extends TypeCodec<String> {
	
	public static final TypeCodec<String> instance = new StringUuidCodec();
	
    private StringUuidCodec() {
		super(DataType.uuid(), String.class);
	}

	@Override
    public String parse(String value) {
        try {
            return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL") ? null : UUID.fromString(value).toString();
        } catch (IllegalArgumentException e) {
            throw new InvalidTypeException(String.format("Cannot parse UUID value from \"%s\"", value), e);
        }
    }

    @Override
    public String format(String value) {
        if (value == null)
            return "NULL";
        return value;
    }

    @Override
    public ByteBuffer serialize(String value, ProtocolVersion protocolVersion) {
        if (value == null)
            return null;
        
        try{
        	final UUID _value = UUID.fromString(value);
        	ByteBuffer bb = ByteBuffer.allocate(16);
        	bb.putLong(0, _value.getMostSignificantBits());
        	bb.putLong(8, _value.getLeastSignificantBits());
        	return bb;
        } catch (IllegalArgumentException e) {
            throw new InvalidTypeException(String.format("Cannot parse UUID value from \"%s\"", value), e);
        }
    }

    @Override
    public String deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return bytes == null || bytes.remaining() == 0 ? null : new UUID(bytes.getLong(bytes.position()), bytes.getLong(bytes.position() + 8)).toString();
    }

}

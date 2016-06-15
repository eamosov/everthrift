package org.everthrift.cassandra.codecs;

import java.nio.ByteBuffer;
import java.text.ParseException;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ParseUtils;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class LongTimestampCodec extends TypeCodec<Long> {

    public static final TypeCodec<Long> instance = new LongTimestampCodec();

    private LongTimestampCodec() {
        super(DataType.timestamp(), Long.class);
    }

    @Override
    public Long parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;
        // strip enclosing single quotes, if any
        if (ParseUtils.isQuoted(value))
            value = ParseUtils.unquote(value);

        if (ParseUtils.isLongLiteral(value)) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
            }
        }

        try {
            return ParseUtils.parseDate(value).getTime();
        } catch (ParseException e) {
            throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
        }
    }

    @Override
    public String format(Long value) {
        if (value == null)
            return "NULL";
        return Long.toString(value);
    }

    @Override
    public ByteBuffer serialize(Long value, ProtocolVersion protocolVersion) {
    	
    	if (value == null)
    		return null;
    	
        final ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(0, value);
        return bb;
    }

    @Override
    public Long deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    	
        if (bytes == null || bytes.remaining() == 0)
            return null;
        
        if (bytes.remaining() != 8)
            throw new InvalidTypeException("Invalid 64-bits long value, expecting 8 bytes but got " + bytes.remaining());

        return bytes.getLong(bytes.position());
    }
}

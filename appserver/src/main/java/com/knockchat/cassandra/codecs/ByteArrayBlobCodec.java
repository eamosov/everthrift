package com.knockchat.cassandra.codecs;

import java.nio.ByteBuffer;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.utils.Bytes;

public class ByteArrayBlobCodec extends TypeCodec<byte[]> {
    public static final ByteArrayBlobCodec instance = new ByteArrayBlobCodec();

    private ByteArrayBlobCodec() {
        super(DataType.blob(), byte[].class);
    }

    @Override
    public byte[] parse(String value) {
        return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL") ? null : Bytes.fromHexString(value).array();
    }

    @Override
    public String format(byte[] value) {
        if (value == null)
            return "NULL";
        return Bytes.toHexString(value);
    }

    @Override
    public ByteBuffer serialize(byte[] value, ProtocolVersion protocolVersion) {
        return value == null ? null : ByteBuffer.wrap(value);
    }

    @Override
    public byte[] deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return bytes == null ? null : bytes.array();
    }

}

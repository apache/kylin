package org.apache.kylin.metadata.serializer;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;

public class StringSerializer extends DataTypeSerializer<String> {

    @Override
    public void serialize(String value, ByteBuffer out) {
        BytesUtil.writeUTFString(value, out);
    }

    @Override
    public String deserialize(ByteBuffer in) {
        return BytesUtil.readUTFString(in);
    }

    @Override
    public int peekLength(ByteBuffer in) {
        return BytesUtil.peekByteArrayLength(in);
    }

    @Override
    public String valueOf(byte[] value) {
        return Bytes.toString(value);
    }

}

package org.apache.kylin.metadata.serializer;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.model.DataType;

public class StringSerializer extends DataTypeSerializer<String> {

    final DataType type;
    final int maxLength;

    public StringSerializer(DataType type) {
        this.type = type;
        // see serialize(): 2 byte length, rest is String.toBytes()
        this.maxLength = 2 + type.getPrecision();
    }

    @Override
    public void serialize(String value, ByteBuffer out) {
        int start = out.position();

        BytesUtil.writeUTFString(value, out);

        if (out.position() - start > maxLength)
            throw new IllegalArgumentException("'" + value + "' exceeds the expected length for type " + type);
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
    public int maxLength() {
        return maxLength;
    }

    @Override
    public String valueOf(byte[] value) {
        return Bytes.toString(value);
    }

}

package org.apache.kylin.metadata.serializer;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.util.DateFormat;

public class DateTimeSerializer extends DataTypeSerializer<LongWritable> {
    
    // avoid mass object creation
    LongWritable current = new LongWritable();

    public DateTimeSerializer(DataType type) {
    }

    @Override
    public void serialize(LongWritable value, ByteBuffer out) {
        out.putLong(value.get());
    }

    @Override
    public LongWritable deserialize(ByteBuffer in) {
        current.set(in.getLong());
        return current;
    }

    @Override
    public int peekLength(ByteBuffer in) {
        return 8;
    }
    
    @Override
    public int maxLength() {
        return 8;
    }

    @Override
    public LongWritable valueOf(byte[] value) {
        if (value == null)
            current.set(0L);
        else
            current.set(DateFormat.stringToMillis(Bytes.toString(value)));
        return current;
    }

}

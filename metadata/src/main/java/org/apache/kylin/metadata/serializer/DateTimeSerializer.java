package org.apache.kylin.metadata.serializer;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.common.util.DateFormat;

public class DateTimeSerializer extends DataTypeSerializer<LongWritable> {
    
    // be thread-safe and avoid repeated obj creation
    private ThreadLocal<LongWritable> current = new ThreadLocal<LongWritable>();

    public DateTimeSerializer(DataType type) {
    }

    @Override
    public void serialize(LongWritable value, ByteBuffer out) {
        out.putLong(value.get());
    }

    private LongWritable current() {
        LongWritable l = current.get();
        if (l == null) {
            l = new LongWritable();
            current.set(l);
        }
        return l;
    }
    
    @Override
    public LongWritable deserialize(ByteBuffer in) {
        LongWritable l = current();
        l.set(in.getLong());
        return l;
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
        LongWritable l = current();
        if (value == null)
            l.set(0L);
        else
            l.set(DateFormat.stringToMillis(Bytes.toString(value)));
        return l;
    }

}

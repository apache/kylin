package org.apache.kylin.aggregation.basic;

import java.nio.ByteBuffer;

import org.apache.kylin.aggregation.DataTypeSerializer;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.LongMutable;
import org.apache.kylin.metadata.model.DataType;

public class DateTimeSerializer extends DataTypeSerializer<LongMutable> {

    // be thread-safe and avoid repeated obj creation
    private ThreadLocal<LongMutable> current = new ThreadLocal<LongMutable>();

    public DateTimeSerializer(DataType type) {
    }

    @Override
    public void serialize(LongMutable value, ByteBuffer out) {
        out.putLong(value.get());
    }

    private LongMutable current() {
        LongMutable l = current.get();
        if (l == null) {
            l = new LongMutable();
            current.set(l);
        }
        return l;
    }

    @Override
    public LongMutable deserialize(ByteBuffer in) {
        LongMutable l = current();
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
    public int getStorageBytesEstimate() {
        return 8;
    }

    @Override
    public LongMutable valueOf(byte[] value) {
        LongMutable l = current();
        if (value == null)
            l.set(0L);
        else
            l.set(DateFormat.stringToMillis(Bytes.toString(value)));
        return l;
    }

}

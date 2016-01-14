package org.apache.kylin.metadata.measure.serializer;

import java.nio.ByteBuffer;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.metadata.measure.LongMutable;
import org.apache.kylin.metadata.model.DataType;

public class BooleanSerializer extends DataTypeSerializer<LongMutable> {

    final String[] TRUE_VALUE_SET = { "true", "t", "on", "yes" };

    // be thread-safe and avoid repeated obj creation
    private ThreadLocal<LongMutable> current = new ThreadLocal<LongMutable>();

    public BooleanSerializer(DataType type) {
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
            l.set(BooleanUtils.toInteger(ArrayUtils.contains(TRUE_VALUE_SET, Bytes.toString(value).toLowerCase())));
        return l;
    }
}

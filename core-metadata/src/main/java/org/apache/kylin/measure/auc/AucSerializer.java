package org.apache.kylin.measure.auc;

import org.apache.kylin.metadata.datatype.DataTypeSerializer;

import java.nio.ByteBuffer;

public class AucSerializer  extends DataTypeSerializer<AucCounter> {
    @Override
    public int peekLength(ByteBuffer in) {
        return 0;
    }

    @Override
    public int maxLength() {
        return 0;
    }

    @Override
    public int getStorageBytesEstimate() {
        return 0;
    }

    @Override
    public void serialize(AucCounter value, ByteBuffer out) {

    }

    @Override
    public AucCounter deserialize(ByteBuffer in) {
        return null;
    }
}

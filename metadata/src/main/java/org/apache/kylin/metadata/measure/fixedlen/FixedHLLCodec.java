package org.apache.kylin.metadata.measure.fixedlen;

import java.nio.ByteBuffer;

import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.metadata.model.DataType;

/**
 */
public class FixedHLLCodec extends FixedLenMeasureCodec<HyperLogLogPlusCounter> {

    private DataType type;
    private int presision;
    private HyperLogLogPlusCounter current;

    public FixedHLLCodec(DataType type) {
        this.type = type;
        this.presision = type.getPrecision();
        this.current = new HyperLogLogPlusCounter(this.presision);
    }

    @Override
    public int getLength() {
        return 1 << presision;
    }

    @Override
    public DataType getDataType() {
        return type;
    }

    @Override
    public HyperLogLogPlusCounter valueOf(String value) {
        current.clear();
        if (value == null)
            current.add("__nUlL__");
        else
            current.add(value.getBytes());
        return current;
    }

    @Override
    public Object getValue() {
        return current;
    }

    @Override
    public HyperLogLogPlusCounter read(byte[] buf, int offset) {
        current.readRegistersArray(ByteBuffer.wrap(buf, offset, buf.length - offset));
        return current;
    }

    @Override
    public void write(HyperLogLogPlusCounter v, byte[] buf, int offset) {
        v.writeRegistersArray(ByteBuffer.wrap(buf, offset, buf.length - offset));
    }
}

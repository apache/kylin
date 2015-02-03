package org.apache.kylin.metadata.measure.fixedlen;

import org.apache.kylin.metadata.model.DataType;
import org.apache.hadoop.io.LongWritable;

import org.apache.kylin.common.util.BytesUtil;

import java.math.BigDecimal;

public class FixedPointLongCodec extends FixedLenMeasureCodec<LongWritable> {

    private static final int SIZE = 8;
    // number of digits after decimal point
    int scale;
    BigDecimal scalePower;
    DataType type;
    // avoid mass object creation
    LongWritable current = new LongWritable();

    public FixedPointLongCodec(DataType type) {
        this.type = type;
        this.scale = Math.max(0, type.getScale());
        this.scalePower = new BigDecimal((long) Math.pow(10, scale));
    }

    @Override
    public int getLength() {
        return SIZE;
    }

    @Override
    public DataType getDataType() {
        return type;
    }

    @Override
    public LongWritable valueOf(String value) {
        if (value == null)
            current.set(0L);
        else
            current.set((new BigDecimal(value).multiply(scalePower).longValue()));
        return current;
    }

    @Override
    public String toString(LongWritable value) {
        if (scale == 0)
            return value.toString();
        else
            return "" + (new BigDecimal(value.get()).divide(scalePower));
    }

    @Override
    public LongWritable read(byte[] buf, int offset) {
        current.set(BytesUtil.readLong(buf, offset, SIZE));
        return current;
    }

    @Override
    public void write(LongWritable v, byte[] buf, int offset) {
        BytesUtil.writeLong(v == null ? 0 : v.get(), buf, offset, SIZE);
    }
}

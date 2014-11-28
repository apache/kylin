package com.kylinolap.cube.measure.fixedlen;

import com.kylinolap.metadata.model.DataType;
import org.apache.hadoop.io.LongWritable;

import com.kylinolap.common.util.BytesUtil;

public class FixedPointLongCodec extends FixedLenMeasureCodec<LongWritable> {

    private static final int SIZE = 8;
    // number of digits after decimal point
    int scale;
    double scalePower;
    DataType type;
    // avoid mass object creation
    LongWritable current = new LongWritable();


    public FixedPointLongCodec(DataType type) {
        this.type = type;
        this.scale = Math.max(0, type.getScale());
        this.scalePower = Math.pow(10, scale);
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
            current.set((long) (Double.parseDouble(value) * scalePower));
        return current;
    }

    @Override
    public String toString(LongWritable value) {
        if (scale == 0)
            return value.toString();
        else
            return "" + (value.get() / scalePower);
    }

    @Override
    public LongWritable read(byte[] buf, int offset) {
        current.set(BytesUtil.readLong(buf, offset, SIZE));
        return current;
    }

    @Override
    public void write(LongWritable v, byte[] buf, int offset) {
        BytesUtil.writeLong(v.get(), buf, offset, SIZE);
    }
}

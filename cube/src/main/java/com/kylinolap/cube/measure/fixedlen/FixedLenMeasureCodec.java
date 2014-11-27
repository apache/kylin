package com.kylinolap.cube.measure.fixedlen;

import com.kylinolap.metadata.model.DataType;

abstract public class FixedLenMeasureCodec<T> {

    public static FixedLenMeasureCodec<?> get(DataType type) {
        return new FixedPointLongCodec(type);
    }

    abstract public int getLength();

    abstract public DataType getDataType();

    abstract public T valueOf(String value);

    abstract public String toString(T value);

    abstract public T read(byte[] buf, int offset);

    abstract public void write(T v, byte[] buf, int offset);

}

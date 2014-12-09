package com.kylinolap.cube.invertedindex;


import com.kylinolap.common.util.BytesSerializer;
import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.cube.measure.fixedlen.FixedLenMeasureCodec;
import com.kylinolap.metadata.model.DataType;
import org.apache.hadoop.io.LongWritable;

import java.nio.ByteBuffer;

/**
 * Created by honma on 11/10/14.
 */
public class TableRecordInfoDigest implements TableRecordFactory {

    protected int nColumns;
    protected int byteFormLen;

    protected int[] offsets;//column offset in byte form row
    protected int[] dictMaxIds;//max id for each of the dict
    protected int[] lengths;//length of each encoded dict
    protected boolean[] isMetric;//whether it's metric or dict

    protected FixedLenMeasureCodec<?>[] measureSerializers;

    public int getByteFormLen() {
        return byteFormLen;
    }

    public boolean isMetrics(int col) {
        return isMetric[col];
    }

    public int getColumnCount() {
        return nColumns;
    }

    public int offset(int col) {
        return offsets[col];
    }

    public int length(int col) {
        return lengths[col];
    }

    public int getMaxID(int col) {
        return dictMaxIds[col];
    }

    public int getMetricCount() {
        int ret = 0;
        for (int i = 0; i < nColumns; ++i) {
            if (isMetrics(i)) {
                ret++;
            }
        }
        return ret;
    }

    @Override
    public TableRecordBytes createTableRecord() {
        return new TableRecordBytes(this);
    }


    // metrics go with fixed-len codec
    @SuppressWarnings("unchecked")
    public FixedLenMeasureCodec<LongWritable> codec(int col) {
        // yes, all metrics are long currently
        return (FixedLenMeasureCodec<LongWritable>) measureSerializers[col];
    }


    public static byte[] serialize(TableRecordInfoDigest o) {
        ByteBuffer buf = ByteBuffer.allocate(Serializer.SERIALIZE_BUFFER_SIZE);
        serializer.serialize(o, buf);
        byte[] result = new byte[buf.position()];
        System.arraycopy(buf.array(), 0, result, 0, buf.position());
        return result;
    }


    public static TableRecordInfoDigest deserialize(byte[] bytes) {
        return serializer.deserialize(ByteBuffer.wrap(bytes));
    }

    public static TableRecordInfoDigest deserialize(ByteBuffer buffer) {
        return serializer.deserialize(buffer);
    }

    private static final Serializer serializer = new Serializer();

    private static class Serializer implements BytesSerializer<TableRecordInfoDigest> {

        @Override
        public void serialize(TableRecordInfoDigest value, ByteBuffer out) {
            BytesUtil.writeVInt(value.nColumns, out);
            BytesUtil.writeVInt(value.byteFormLen, out);
            BytesUtil.writeIntArray(value.offsets, out);
            BytesUtil.writeIntArray(value.dictMaxIds, out);
            BytesUtil.writeIntArray(value.lengths, out);

            for (int i = 0; i < value.measureSerializers.length; ++i) {
                BytesUtil.writeAsciiString(value.measureSerializers[i].getDataType().toString(), out);
            }
        }

        @Override
        public TableRecordInfoDigest deserialize(ByteBuffer in) {
            TableRecordInfoDigest result = new TableRecordInfoDigest();
            result.nColumns = BytesUtil.readVInt(in);
            result.byteFormLen = BytesUtil.readVInt(in);
            result.offsets = BytesUtil.readIntArray(in);
            result.dictMaxIds = BytesUtil.readIntArray(in);
            result.lengths = BytesUtil.readIntArray(in);

            result.measureSerializers = new FixedLenMeasureCodec<?>[result.nColumns];
            for (int i = 0; i < result.nColumns; ++i) {
                String typeStr = BytesUtil.readAsciiString(in);
                if (typeStr == null) {
                    result.measureSerializers[i] = null;
                } else {
                    result.measureSerializers[i] = FixedLenMeasureCodec.get(DataType.getInstance(typeStr));
                }
            }

            return result;
        }


    }
}

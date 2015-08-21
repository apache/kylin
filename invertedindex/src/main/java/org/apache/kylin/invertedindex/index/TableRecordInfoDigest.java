/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.invertedindex.index;

import java.nio.ByteBuffer;

import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.measure.fixedlen.FixedLenMeasureCodec;
import org.apache.kylin.metadata.model.DataType;

/**
 * Created by honma on 11/10/14.
 */
public class TableRecordInfoDigest {

    private int nColumns;
    private int byteFormLen;

    private int[] offsets;// column offset in byte form row
    private int[] dictMaxIds;// max id for each of the dict
    private int[] lengths;// length of each encoded dict
    private boolean[] isMetric;// whether it's metric or dict

    protected FixedLenMeasureCodec<?>[] measureSerializers;

    public TableRecordInfoDigest(int nColumns, int byteFormLen, int[] offsets, int[] dictMaxIds, int[] lengths, boolean[] isMetric, FixedLenMeasureCodec<?>[] measureSerializers) {
        this.nColumns = nColumns;
        this.byteFormLen = byteFormLen;
        this.offsets = offsets;
        this.dictMaxIds = dictMaxIds;
        this.lengths = lengths;
        this.isMetric = isMetric;
        this.measureSerializers = measureSerializers;
    }

    public TableRecordInfoDigest() {
    }

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

    public RawTableRecord createTableRecordBytes() {
        return new RawTableRecord(this);
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
            BytesUtil.writeBooleanArray(value.isMetric, out);

            for (int i = 0; i < value.measureSerializers.length; ++i) {
                if (value.isMetrics(i)) {
                    BytesUtil.writeAsciiString(value.measureSerializers[i].getDataType().toString(), out);
                } else {
                    BytesUtil.writeAsciiString(null, out);
                }
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
            result.isMetric = BytesUtil.readBooleanArray(in);

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

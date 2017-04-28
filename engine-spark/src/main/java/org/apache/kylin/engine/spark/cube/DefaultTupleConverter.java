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
package org.apache.kylin.engine.spark.cube;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.cube.kv.RowKeyEncoderProvider;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.metadata.model.TblColRef;

import scala.Tuple2;

/**
 */
public final class DefaultTupleConverter implements TupleConverter {

    private final static transient ThreadLocal<ByteBuffer> valueBuf = new ThreadLocal<>();
    private final CubeSegment segment;
    private final int measureCount;
    private final Map<TblColRef, Integer> columnLengthMap;
    private RowKeyEncoderProvider rowKeyEncoderProvider;
    private byte[] rowKeyBodyBuf = new byte[RowConstants.ROWKEY_BUFFER_SIZE];

    public DefaultTupleConverter(CubeSegment segment, Map<TblColRef, Integer> columnLengthMap) {
        this.segment = segment;
        this.measureCount = segment.getCubeDesc().getMeasures().size();
        this.columnLengthMap = columnLengthMap;
        this.rowKeyEncoderProvider = new RowKeyEncoderProvider(this.segment);
    }

    private ByteBuffer getValueBuf() {
        if (valueBuf.get() == null) {
            valueBuf.set(ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE));
        }
        return valueBuf.get();
    }

    private void setValueBuf(ByteBuffer buf) {
        valueBuf.set(buf);
    }

    @Override
    public final Tuple2<byte[], byte[]> convert(long cuboidId, GTRecord record) {
        Cuboid cuboid = Cuboid.findById(segment.getCubeDesc(), cuboidId);
        RowKeyEncoder rowkeyEncoder = rowKeyEncoderProvider.getRowkeyEncoder(cuboid);

        final int dimensions = Long.bitCount(cuboidId);
        final ImmutableBitSet measureColumns = new ImmutableBitSet(dimensions, dimensions + measureCount);

        int offSet = 0;
        for (int x = 0; x < dimensions; x++) {
            final ByteArray byteArray = record.get(x);
            System.arraycopy(byteArray.array(), byteArray.offset(), rowKeyBodyBuf, offSet, byteArray.length());
            offSet += byteArray.length();
        }

        byte[] rowKey = rowkeyEncoder.createBuf();
        rowkeyEncoder.encode(new ByteArray(rowKeyBodyBuf, 0, offSet), new ByteArray(rowKey));

        ByteBuffer valueBuf = getValueBuf();
        valueBuf.clear();
        try {
            record.exportColumns(measureColumns, valueBuf);
        } catch (BufferOverflowException boe) {
            valueBuf = ByteBuffer.allocate((int) (record.sizeOf(measureColumns) * 1.5));
            record.exportColumns(measureColumns, valueBuf);
            setValueBuf(valueBuf);
        }

        byte[] value = new byte[valueBuf.position()];
        System.arraycopy(valueBuf.array(), 0, value, 0, valueBuf.position());
        return new Tuple2<>(rowKey, value);
    }
}

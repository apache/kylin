/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.storage.hbase.coprocessor;

import com.kylinolap.common.util.BytesSerializer;
import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.cube.kv.RowKeyEncoder;
import com.kylinolap.metadata.model.cube.TblColRef;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * @author yangli9
 */
public class RowProjector {

    public static RowProjector fromColumns(final CubeSegment cubeSegment, final Cuboid cuboid,
                                           final Collection<TblColRef> dimensionColumns) {

        RowKeyEncoder rowKeyMaskEncoder = new RowKeyEncoder(cubeSegment, cuboid) {
            @Override
            protected int fillHeader(byte[] bytes, byte[][] values) {
                // always keep header, coz with-header cube is only selected when header-column is needed
                // (otherwise the non-header cube should be selected)
                Arrays.fill(bytes, 0, this.headerLength, (byte) 0xff);
                return this.headerLength;
            }

            @Override
            protected void fillColumnValue(TblColRef column, int columnLen, byte[] value, int valueLen,
                                           byte[] outputValue, int outputValueOffset) {
                byte bits = dimensionColumns.contains(column) ? (byte) 0xff : 0x00;
                Arrays.fill(outputValue, outputValueOffset, outputValueOffset + columnLen, bits);
            }
        };

        byte[] mask = rowKeyMaskEncoder.encode(new byte[cuboid.getColumns().size()][]);
        return new RowProjector(mask);
    }

    public static byte[] serialize(RowProjector o) {
        ByteBuffer buf = ByteBuffer.allocate(CoprocessorEnabler.SERIALIZE_BUFFER_SIZE);
        serializer.serialize(o, buf);
        byte[] result = new byte[buf.position()];
        System.arraycopy(buf.array(), 0, result, 0, buf.position());
        return result;
    }

    public static RowProjector deserialize(byte[] bytes) {
        return serializer.deserialize(ByteBuffer.wrap(bytes));
    }

    private static final Serializer serializer = new Serializer();

    private static class Serializer implements BytesSerializer<RowProjector> {

        @Override
        public void serialize(RowProjector value, ByteBuffer out) {
            BytesUtil.writeVInt(value.length, out);
            out.put(value.rowKeyBitMask);
        }

        @Override
        public RowProjector deserialize(ByteBuffer in) {
            int len = BytesUtil.readVInt(in);
            byte[] mask = new byte[len];
            in.get(mask);
            return new RowProjector(mask);
        }
    }

    // ============================================================================

    final int length;
    final byte[] rowKeyBitMask; // mask out columns that are not needed (by group by)

    public RowProjector(byte[] rowKeyBitMask) {
        this.length = rowKeyBitMask.length;
        this.rowKeyBitMask = rowKeyBitMask;
    }

    public ImmutableBytesWritable getRowKey(List<Cell> rowCells) {
        Cell cell = rowCells.get(0);
        assert length == cell.getRowLength();

        byte[] result = new byte[length];
        System.arraycopy(cell.getRowArray(), cell.getRowOffset(), result, 0, length);

        for (int i = 0, n = length; i < n; i++) {
            result[i] &= rowKeyBitMask[i];
        }

        return new ImmutableBytesWritable(result);
    }

}

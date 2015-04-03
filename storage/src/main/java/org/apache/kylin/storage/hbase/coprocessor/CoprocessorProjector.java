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

package org.apache.kylin.storage.hbase.coprocessor;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * @author yangli9
 */
public class CoprocessorProjector {

    public static CoprocessorProjector makeForObserver(final CubeSegment cubeSegment, final Cuboid cuboid, final Collection<TblColRef> dimensionColumns) {

        RowKeyEncoder rowKeyMaskEncoder = new RowKeyEncoder(cubeSegment, cuboid) {
            @Override
            protected int fillHeader(byte[] bytes, byte[][] values) {
                Arrays.fill(bytes, 0, this.headerLength, (byte) 0xff);
                return this.headerLength;
            }

            @Override
            protected void fillColumnValue(TblColRef column, int columnLen, byte[] value, int valueLen, byte[] outputValue, int outputValueOffset) {
                byte bits = dimensionColumns.contains(column) ? (byte) 0xff : 0x00;
                Arrays.fill(outputValue, outputValueOffset, outputValueOffset + columnLen, bits);
            }
        };

        byte[] mask = rowKeyMaskEncoder.encode(new byte[cuboid.getColumns().size()][]);
        return new CoprocessorProjector(mask);
    }

    public static CoprocessorProjector makeForEndpoint(final TableRecordInfo tableInfo, final Collection<TblColRef> groupby) {
        byte[] mask = new byte[tableInfo.getDigest().getByteFormLen()];
        int maskIdx = 0;
        for (int i = 0; i < tableInfo.getDigest().getColumnCount(); ++i) {
            TblColRef tblColRef = tableInfo.getColumns().get(i);
            int length = tableInfo.getDigest().length(i);
            byte bits = groupby.contains(tblColRef) ? (byte) 0xff : 0x00;
            for (int j = 0; j < length; ++j) {
                mask[maskIdx++] = bits;
            }
        }
        return new CoprocessorProjector(mask);
    }

    public static byte[] serialize(CoprocessorProjector o) {
        ByteBuffer buf = ByteBuffer.allocate(CoprocessorConstants.SERIALIZE_BUFFER_SIZE);
        serializer.serialize(o, buf);
        byte[] result = new byte[buf.position()];
        System.arraycopy(buf.array(), 0, result, 0, buf.position());
        return result;
    }

    public static CoprocessorProjector deserialize(byte[] bytes) {
        return serializer.deserialize(ByteBuffer.wrap(bytes));
    }

    private static final Serializer serializer = new Serializer();

    private static class Serializer implements BytesSerializer<CoprocessorProjector> {

        @Override
        public void serialize(CoprocessorProjector value, ByteBuffer out) {
            BytesUtil.writeByteArray(value.groupByMask, out);
        }

        @Override
        public CoprocessorProjector deserialize(ByteBuffer in) {
            byte[] mask = BytesUtil.readByteArray(in);
            return new CoprocessorProjector(mask);
        }
    }

    // ============================================================================

    final AggrKey aggrKey = new AggrKey();
    final byte[] groupByMask; // mask out columns that are not needed (by group by)
    final BitSet groupByMaskSet;

    public CoprocessorProjector(byte[] groupByMask) {
        this.groupByMask = groupByMask;
        this.groupByMaskSet = new BitSet();
        for (int i = 0; i < groupByMask.length; i++) {
            if (groupByMask[i] != 0) {
                groupByMaskSet.set(i);
            }
        }
    }

    public AggrKey getAggrKey(List<Cell> rowCells) {
        Cell cell = rowCells.get(0);
        assert groupByMask.length == cell.getRowLength();

        aggrKey.set(cell.getRowArray(), cell.getRowOffset());
        return aggrKey;
    }

    public AggrKey getAggrKey(byte[] row) {
        assert groupByMask.length == row.length;
        aggrKey.set(row, 0);
        return aggrKey;
    }

    public class AggrKey implements Comparable<AggrKey> {
        byte[] data;
        int offset;

        public byte[] get() {
            return data;
        }

        public int offset() {
            return offset;
        }

        public int length() {
            return groupByMask.length;
        }

        void set(byte[] data, int offset) {
            this.data = data;
            this.offset = offset;
        }

        public AggrKey copy() {
            AggrKey copy = new AggrKey();
            copy.set(new byte[length()], 0);
            System.arraycopy(this.data, this.offset, copy.data, copy.offset, length());
            return copy;
        }

        @Override
        public int hashCode() {
            int hash = 1;
            for (int i = groupByMaskSet.nextSetBit(0); i >= 0; i = groupByMaskSet.nextSetBit(i + 1)) {
                hash = (31 * hash) + data[offset + i];
            }
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            AggrKey other = (AggrKey) obj;
            if (this.length() != other.length())
                return false;

            return compareTo(other) == 0;
        }

        @Override
        public int compareTo(AggrKey o) {
            int comp = this.length() - o.length();
            if (comp != 0)
                return comp;

            for (int i = groupByMaskSet.nextSetBit(0); i >= 0; i = groupByMaskSet.nextSetBit(i + 1)) {
                comp = BytesUtil.compareByteUnsigned(this.data[this.offset + i], o.data[o.offset + i]);
                if (comp != 0)
                    return comp;
            }
            return 0;
        }
    }
    
}

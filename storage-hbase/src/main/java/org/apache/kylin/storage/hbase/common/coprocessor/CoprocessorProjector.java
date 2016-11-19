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

package org.apache.kylin.storage.hbase.common.coprocessor;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * @author yangli9
 */
public class CoprocessorProjector {

    public static CoprocessorProjector makeForObserver(final CubeSegment cubeSegment, final Cuboid cuboid, final Collection<TblColRef> dimensionColumns) {

        RowKeyEncoder rowKeyMaskEncoder = new RowKeyEncoder(cubeSegment, cuboid) {
            @Override
            protected void fillHeader(byte[] bytes) {
                Arrays.fill(bytes, 0, this.getHeaderLength(), (byte) 0xff);
            }

            @Override
            protected void fillColumnValue(TblColRef column, int columnLen, String valueStr, byte[] outputValue, int outputValueOffset) {
                byte bits = dimensionColumns.contains(column) ? (byte) 0xff : 0x00;
                Arrays.fill(outputValue, outputValueOffset, outputValueOffset + columnLen, bits);
            }
        };

        byte[] mask = rowKeyMaskEncoder.encode(new String[cuboid.getColumns().size()]);
        return new CoprocessorProjector(mask, dimensionColumns.size() != 0);
    }
  

    public static byte[] serialize(CoprocessorProjector o) {
        ByteBuffer buf = ByteBuffer.allocate(BytesSerializer.SERIALIZE_BUFFER_SIZE);
        serializer.serialize(o, buf);
        byte[] result = new byte[buf.position()];
        System.arraycopy(buf.array(), 0, result, 0, buf.position());
        return result;
    }

    public static CoprocessorProjector deserialize(byte[] bytes) {
        return serializer.deserialize(ByteBuffer.wrap(bytes));
    }

    private static final BytesSerializer<CoprocessorProjector> serializer = new BytesSerializer<CoprocessorProjector>() {
        @Override
        public void serialize(CoprocessorProjector value, ByteBuffer out) {
            BytesUtil.writeByteArray(value.groupByMask, out);
            BytesUtil.writeVInt(value.hasGroupby ? 1 : 0, out);
        }

        @Override
        public CoprocessorProjector deserialize(ByteBuffer in) {
            byte[] mask = BytesUtil.readByteArray(in);
            boolean hasGroupBy = BytesUtil.readVInt(in) == 1;
            return new CoprocessorProjector(mask, hasGroupBy);
        }
    };

    // ============================================================================

    final transient AggrKey aggrKey;
    final byte[] groupByMask; // mask out columns that are not needed (by group by)
    final boolean hasGroupby;

    public CoprocessorProjector(byte[] groupByMask, boolean hasGroupby) {
        this.groupByMask = groupByMask;
        this.aggrKey = new AggrKey(groupByMask);
        this.hasGroupby = hasGroupby;
    }

    public boolean hasGroupby() {
        return hasGroupby;
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

}

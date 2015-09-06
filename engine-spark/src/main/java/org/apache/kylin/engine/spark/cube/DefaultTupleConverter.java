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

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.metadata.model.TblColRef;
import scala.Tuple2;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Map;

/**
 */
public final class DefaultTupleConverter implements TupleConverter {

    private final static ThreadLocal<ByteBuffer> valueBuf = new ThreadLocal<>();
    private final static ThreadLocal<int[]> measureColumnsIndex = new ThreadLocal<>();
    private final CubeDesc cubeDesc;
    private final int measureCount;
    private final Map<TblColRef, Integer> columnLengthMap;

    public DefaultTupleConverter(CubeDesc cubeDesc, Map<TblColRef, Integer> columnLengthMap) {
        this.cubeDesc = cubeDesc;
        this.measureCount = cubeDesc.getMeasures().size();
        this.columnLengthMap = columnLengthMap;
    }

    private ByteBuffer getValueBuf() {
        if (valueBuf.get() == null) {
            valueBuf.set(ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE));
        }
        return valueBuf.get();
    }

    private int[] getMeasureColumnsIndex() {
        if (measureColumnsIndex.get() == null) {
            measureColumnsIndex.set(new int[measureCount]);
        }
        return measureColumnsIndex.get();
    }
    
    @Override
    public final Tuple2<byte[], byte[]> convert(long cuboidId, GTRecord record) {
        int bytesLength = RowConstants.ROWKEY_CUBOIDID_LEN;
        Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidId);
        for (TblColRef column : cuboid.getColumns()) {
            bytesLength += columnLengthMap.get(column);
        }

        final int dimensions = BitSet.valueOf(new long[]{cuboidId}).cardinality();
        int[] measureColumnsIndex = getMeasureColumnsIndex();
        for (int i = 0; i < measureCount; i++) {
            measureColumnsIndex[i] = dimensions + i;
        }

        byte[] key = new byte[bytesLength];
        System.arraycopy(Bytes.toBytes(cuboidId), 0, key, 0, RowConstants.ROWKEY_CUBOIDID_LEN);
        int offSet = RowConstants.ROWKEY_CUBOIDID_LEN;
        for (int x = 0; x < dimensions; x++) {
            final ByteArray byteArray = record.get(x);
            System.arraycopy(byteArray.array(), byteArray.offset(), key, offSet, byteArray.length());
            offSet += byteArray.length();
        }

        ByteBuffer valueBuf = getValueBuf();
        valueBuf.clear();
        record.exportColumns(measureColumnsIndex, valueBuf);

        byte[] value = new byte[valueBuf.position()];
        System.arraycopy(valueBuf.array(), 0, value, 0, valueBuf.position());
        return new Tuple2<>(key, value);
    }
}

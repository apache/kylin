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

package org.apache.kylin.cube.common;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.SplittedBytes;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.metadata.model.TblColRef;

public class RowKeySplitter implements java.io.Serializable {

    private CubeDesc cubeDesc;
    private RowKeyColumnIO colIO;

    private SplittedBytes[] splitBuffers;
    private int[] splitOffsets;
    private int bufferSize;

    private boolean enableSharding;
    private short shardId;

    public SplittedBytes[] getSplitBuffers() {
        return splitBuffers;
    }

    public int[] getSplitOffsets() {
        return splitOffsets;
    }

    public int getBodySplitOffset() {
        if (enableSharding) {
            return 2;//shard+cuboid
        } else {
            return 1;//cuboid
        }
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public RowKeySplitter(CubeSegment cubeSeg, int splitLen, int bytesLen) {
        this.enableSharding = cubeSeg.isEnableSharding();
        this.cubeDesc = cubeSeg.getCubeDesc();
        IDimensionEncodingMap dimEncoding = new CubeDimEncMap(cubeSeg);

        for (RowKeyColDesc rowKeyColDesc : cubeDesc.getRowkey().getRowKeyColumns()) {
            dimEncoding.get(rowKeyColDesc.getColRef());
        }

        this.colIO = new RowKeyColumnIO(dimEncoding);

        this.splitBuffers = new SplittedBytes[splitLen];
        this.splitOffsets = new int[splitLen];
        for (int i = 0; i < splitLen; i++) {
            this.splitBuffers[i] = new SplittedBytes(bytesLen);
        }
        this.bufferSize = 0;
    }

    public Short getShardId() {
        if (enableSharding) {
            return shardId;
        }
        return null;
    }

    /**
     * @param bytes
     * @return cuboid ID
     */
    public long split(byte[] bytes) {
        this.bufferSize = 0;
        int offset = 0;

        if (enableSharding) {
            // extract shard
            SplittedBytes shardSplit = this.splitBuffers[this.bufferSize++];
            shardSplit.length = RowConstants.ROWKEY_SHARDID_LEN;
            System.arraycopy(bytes, offset, shardSplit.value, 0, RowConstants.ROWKEY_SHARDID_LEN);
            offset += RowConstants.ROWKEY_SHARDID_LEN;
            //lastSplittedShard = Bytes.toShort(shardSplit.value, 0, shardSplit.length);
            shardId = Bytes.toShort(shardSplit.value);
        }

        // extract cuboid id
        SplittedBytes cuboidIdSplit = this.splitBuffers[this.bufferSize++];
        cuboidIdSplit.length = RowConstants.ROWKEY_CUBOIDID_LEN;
        System.arraycopy(bytes, offset, cuboidIdSplit.value, 0, RowConstants.ROWKEY_CUBOIDID_LEN);
        offset += RowConstants.ROWKEY_CUBOIDID_LEN;

        long lastSplittedCuboidId = Bytes.toLong(cuboidIdSplit.value, 0, cuboidIdSplit.length);
        Cuboid cuboid = Cuboid.findById(cubeDesc, lastSplittedCuboidId);

        // rowkey columns
        for (int i = 0; i < cuboid.getColumns().size(); i++) {
            splitOffsets[i] = offset;
            TblColRef col = cuboid.getColumns().get(i);
            int colLength = colIO.getColumnLength(col);
            SplittedBytes split = this.splitBuffers[this.bufferSize++];
            split.length = colLength;
            System.arraycopy(bytes, offset, split.value, 0, colLength);
            offset += colLength;
        }

        return lastSplittedCuboidId;
    }
}

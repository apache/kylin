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
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.TblColRef;

public class RowKeySplitter {

    private CubeDesc cubeDesc;
    private RowKeyColumnIO colIO;

    private SplittedBytes[] splitBuffers;
    private int bufferSize;

    private long lastSplittedCuboidId;
    private short lastSplittedShard;

    public SplittedBytes[] getSplitBuffers() {
        return splitBuffers;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public long getLastSplittedCuboidId() {
        return lastSplittedCuboidId;
    }

    public short getLastSplittedShard() {
        return lastSplittedShard;
    }

    public RowKeySplitter(CubeSegment cubeSeg, int splitLen, int bytesLen) {
        this.cubeDesc = cubeSeg.getCubeDesc();
        this.colIO = new RowKeyColumnIO(cubeSeg);

        this.splitBuffers = new SplittedBytes[splitLen];
        for (int i = 0; i < splitLen; i++) {
            this.splitBuffers[i] = new SplittedBytes(bytesLen);
        }
        this.bufferSize = 0;
    }

    /**
     * @param bytes
     * @return cuboid ID
     */
    public long split(byte[] bytes) {
        this.bufferSize = 0;
        int offset = 0;

        // extract shard
        SplittedBytes shardSplit = this.splitBuffers[this.bufferSize++];
        shardSplit.length = RowConstants.ROWKEY_SHARDID_LEN;
        System.arraycopy(bytes, offset, shardSplit.value, 0, RowConstants.ROWKEY_SHARDID_LEN);
        offset += RowConstants.ROWKEY_SHARDID_LEN;

        // extract cuboid id
        SplittedBytes cuboidIdSplit = this.splitBuffers[this.bufferSize++];
        cuboidIdSplit.length = RowConstants.ROWKEY_CUBOIDID_LEN;
        System.arraycopy(bytes, offset, cuboidIdSplit.value, 0, RowConstants.ROWKEY_CUBOIDID_LEN);
        offset += RowConstants.ROWKEY_CUBOIDID_LEN;

        lastSplittedCuboidId = Bytes.toLong(cuboidIdSplit.value, 0, cuboidIdSplit.length);
        lastSplittedShard = Bytes.toShort(shardSplit.value, 0, shardSplit.length);
        Cuboid cuboid = Cuboid.findById(cubeDesc, lastSplittedCuboidId);

        // rowkey columns
        for (int i = 0; i < cuboid.getColumns().size(); i++) {
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

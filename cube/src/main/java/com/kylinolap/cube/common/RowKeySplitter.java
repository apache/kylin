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
package com.kylinolap.cube.common;

import org.apache.hadoop.hbase.util.Bytes;

import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.cube.kv.RowConstants;
import com.kylinolap.cube.kv.RowKeyColumnIO;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.metadata.model.realization.TblColRef;

/**
 * @author George Song (ysong1)
 * 
 */
public class RowKeySplitter {

    private CubeDesc cubeDesc;
    private RowKeyColumnIO colIO;

    private SplittedBytes[] splitBuffers;
    private int bufferSize;

    public SplittedBytes[] getSplitBuffers() {
        return splitBuffers;
    }

    public int getBufferSize() {
        return bufferSize;
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
     * @param byteLen
     * @return cuboid ID
     */
    public long split(byte[] bytes, int byteLen) {
        this.bufferSize = 0;
        int offset = 0;

        // extract cuboid id
        SplittedBytes cuboidIdSplit = this.splitBuffers[this.bufferSize++];
        cuboidIdSplit.length = RowConstants.ROWKEY_CUBOIDID_LEN;
        System.arraycopy(bytes, offset, cuboidIdSplit.value, 0, RowConstants.ROWKEY_CUBOIDID_LEN);
        offset += RowConstants.ROWKEY_CUBOIDID_LEN;

        long cuboidId = Bytes.toLong(cuboidIdSplit.value, 0, cuboidIdSplit.length);
        Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidId);

        // rowkey columns
        for (int i = 0; i < cuboid.getColumns().size(); i++) {
            TblColRef col = cuboid.getColumns().get(i);
            int colLength = colIO.getColumnLength(col);
            SplittedBytes split = this.splitBuffers[this.bufferSize++];
            split.length = colLength;
            System.arraycopy(bytes, offset, split.value, 0, colLength);
            offset += colLength;
        }

        return cuboidId;
    }
}

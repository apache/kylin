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

package org.apache.kylin.engine.mr.common;

import java.io.Serializable;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.cube.kv.RowKeyEncoderProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
@SuppressWarnings("serial")
public class NDCuboidBuilder implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(NDCuboidBuilder.class);
    protected String cubeName;
    protected String segmentID;
    protected CubeSegment cubeSegment;
    private RowKeySplitter rowKeySplitter;
    private RowKeyEncoderProvider rowKeyEncoderProvider;
    private ByteArray newKeyBodyBuf = null;

    public NDCuboidBuilder(CubeSegment cubeSegment) {
        this(cubeSegment, new RowKeyEncoderProvider(cubeSegment));
    }

    public NDCuboidBuilder(CubeSegment cubeSegment, RowKeyEncoderProvider rowKeyEncoderProvider) {
        this.cubeSegment = cubeSegment;
        this.rowKeyEncoderProvider = rowKeyEncoderProvider;
        this.rowKeySplitter = new RowKeySplitter(cubeSegment);
    }

    /**
     * Build the new key, return a reused ByteArray object. Suitable for MR
     * @param parentCuboid
     * @param childCuboid
     * @param splitBuffers
     * @return
     */
    public Pair<Integer, ByteArray> buildKey(Cuboid parentCuboid, Cuboid childCuboid, ByteArray[] splitBuffers) {
        RowKeyEncoder rowkeyEncoder = rowKeyEncoderProvider.getRowkeyEncoder(childCuboid);
        int fullKeySize = rowkeyEncoder.getBytesLength();
        if (newKeyBodyBuf == null || newKeyBodyBuf.length() < fullKeySize) {
            newKeyBodyBuf = new ByteArray(fullKeySize);
        }

        buildKeyInternal(parentCuboid, childCuboid, splitBuffers, newKeyBodyBuf);
        return new Pair<>(Integer.valueOf(fullKeySize), newKeyBodyBuf);

    }

    /**
     * Build the new key, return a new ByteArray object each time. Suitable for spark
     * @param parentCuboid
     * @param childCuboid
     * @param splitBuffers
     * @return
     */
    public ByteArray buildKey2(Cuboid parentCuboid, Cuboid childCuboid, ByteArray[] splitBuffers) {
        RowKeyEncoder rowkeyEncoder = rowKeyEncoderProvider.getRowkeyEncoder(childCuboid);
        int fullKeySize = rowkeyEncoder.getBytesLength();
        ByteArray newKey = new ByteArray(fullKeySize);
        buildKeyInternal(parentCuboid, childCuboid, splitBuffers, newKey);
        return newKey;
    }

    private void buildKeyInternal(Cuboid parentCuboid, Cuboid childCuboid, ByteArray[] splitBuffers, ByteArray newKeyBodyBuf) {
        RowKeyEncoder rowkeyEncoder = rowKeyEncoderProvider.getRowkeyEncoder(childCuboid);

        // rowkey columns
        long mask = Long.highestOneBit(parentCuboid.getId());
        long parentCuboidId = parentCuboid.getId();
        long childCuboidId = childCuboid.getId();
        long parentCuboidIdActualLength = (long)Long.SIZE - Long.numberOfLeadingZeros(parentCuboid.getId());
        int index = rowKeySplitter.getBodySplitOffset(); // skip shard and cuboidId
        int offset = RowConstants.ROWKEY_SHARDID_LEN + RowConstants.ROWKEY_CUBOIDID_LEN; // skip shard and cuboidId
        for (int i = 0; i < parentCuboidIdActualLength; i++) {
            if ((mask & parentCuboidId) > 0) {// if the this bit position equals
                // 1
                if ((mask & childCuboidId) > 0) {// if the child cuboid has this
                    // column
                    System.arraycopy(splitBuffers[index].array(), splitBuffers[index].offset(), newKeyBodyBuf.array(), offset, splitBuffers[index].length());
                    offset += splitBuffers[index].length();
                }
                index++;
            }
            mask = mask >> 1;
        }

        rowkeyEncoder.fillHeader(newKeyBodyBuf.array());
    }

}

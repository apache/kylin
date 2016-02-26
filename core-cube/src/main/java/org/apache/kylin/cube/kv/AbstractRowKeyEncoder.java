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

package org.apache.kylin.cube.kv;

import java.util.Map;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author xjiang
 * 
 */
public abstract class AbstractRowKeyEncoder {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractRowKeyEncoder.class);
    public static final byte DEFAULT_BLANK_BYTE = DimensionEncoding.NULL;

    protected byte blankByte = DEFAULT_BLANK_BYTE;
    protected final CubeSegment cubeSeg;
    protected Cuboid cuboid;

    public static AbstractRowKeyEncoder createInstance(CubeSegment cubeSeg, Cuboid cuboid) {
        return new RowKeyEncoder(cubeSeg, cuboid);
    }

    protected AbstractRowKeyEncoder(CubeSegment cubeSeg, Cuboid cuboid) {
        this.cuboid = cuboid;
        this.cubeSeg = cubeSeg;
    }

    public void setBlankByte(byte blankByte) {
        this.blankByte = blankByte;
    }

    public long getCuboidID() {
        return cuboid.getId();
    }

    public void setCuboid(Cuboid cuboid) {
        this.cuboid = cuboid;
    }

    abstract public byte[] createBuf();

    /**
     * encode a gtrecord into a given byte[] buffer
     * @param record
     * @param keyColumns
     * @param buf
     */
    abstract public void encode(GTRecord record, ImmutableBitSet keyColumns, byte[] buf);

    /**
     * when a rowkey's body is provided, help to encode cuboid & shard (if apply)
     * @param bodyBytes
     * @param outputBuf
     */
    abstract public void encode(ByteArray bodyBytes, ByteArray outputBuf);

    abstract public byte[] encode(Map<TblColRef, String> valueMap);

    abstract public byte[] encode(byte[][] values);
}

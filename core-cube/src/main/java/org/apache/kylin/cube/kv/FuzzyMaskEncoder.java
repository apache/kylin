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

import java.util.Arrays;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * 
 * @author xjiang
 * 
 */
public class FuzzyMaskEncoder extends RowKeyEncoder {

    public FuzzyMaskEncoder(CubeSegment seg, Cuboid cuboid) {
        super(seg, cuboid);
    }

    @Override
    public void encode(GTRecord record, ImmutableBitSet keyColumns, byte[] buf) {
        ByteArray byteArray = new ByteArray(buf, getHeaderLength(), 0);

        GTInfo info = record.getInfo();
        byte fill;
        int pos = 0;
        for (int i = 0; i < info.getPrimaryKey().trueBitCount(); i++) {
            int c = info.getPrimaryKey().trueBitAt(i);
            int colLength = info.getCodeSystem().maxCodeLength(c);

            if (record.get(c).array() != null) {
                fill = RowConstants.BYTE_ZERO;
            } else {
                fill = RowConstants.BYTE_ONE;
            }
            Arrays.fill(byteArray.array(), byteArray.offset() + pos, byteArray.offset() + pos + colLength, fill);
            pos += colLength;
        }
        byteArray.setLength(pos);

        //fill shard and cuboid
        fillHeader(buf);
    }

    @Override
    public void fillHeader(byte[] bytes) {
        int offset = 0;
        if (enableSharding) {
            Arrays.fill(bytes, 0, RowConstants.ROWKEY_SHARDID_LEN, RowConstants.BYTE_ONE);
            offset += RowConstants.ROWKEY_SHARDID_LEN;
        }
        // always fuzzy match cuboid ID to lock on the selected cuboid
        int headerLength = this.getHeaderLength();
        Arrays.fill(bytes, offset, headerLength, RowConstants.BYTE_ZERO);
    }

    @Override
    protected void fillColumnValue(TblColRef column, int columnLen, String valueStr, byte[] outputValue, int outputValueOffset) {
        if (valueStr == null) {
            Arrays.fill(outputValue, outputValueOffset, outputValueOffset + columnLen, RowConstants.BYTE_ONE);
        } else {
            Arrays.fill(outputValue, outputValueOffset, outputValueOffset + columnLen, RowConstants.BYTE_ZERO);
        }
    }
}

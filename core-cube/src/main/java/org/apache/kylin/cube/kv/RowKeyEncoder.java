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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.model.TblColRef;

public class RowKeyEncoder extends AbstractRowKeyEncoder {

    private int bytesLength;
    protected int headerLength;
    private RowKeyColumnIO colIO;
    CubeSegment cubeSeg;

    protected RowKeyEncoder(CubeSegment cubeSeg, Cuboid cuboid) {
        super(cuboid);
        this.cubeSeg = cubeSeg;
        colIO = new RowKeyColumnIO(cubeSeg);
        bytesLength = headerLength = RowConstants.ROWKEY_HEADER_LEN; // include shard and cuboidid 
        for (TblColRef column : cuboid.getColumns()) {
            bytesLength += colIO.getColumnLength(column);
        }
    }

    public int getColumnLength(TblColRef col) {
        return colIO.getColumnLength(col);
    }

    @Override
    public byte[] encode(Map<TblColRef, String> valueMap) {
        List<byte[]> valueList = new ArrayList<byte[]>();
        for (TblColRef bdCol : cuboid.getColumns()) {
            String value = valueMap.get(bdCol);
            valueList.add(valueStringToBytes(value));
        }
        byte[][] values = valueList.toArray(RowConstants.BYTE_ARR_MARKER);
        return encode(values);
    }

    public byte[] valueStringToBytes(String value) {
        if (value == null)
            return null;
        else
            return Bytes.toBytes(value);
    }

    @Override
    public byte[] encode(byte[][] values) {
        byte[] bytes = new byte[this.bytesLength];
        int bodyOffset = RowConstants.ROWKEY_HEADER_LEN;
        int offset = bodyOffset;

        for (int i = 0; i < cuboid.getColumns().size(); i++) {
            TblColRef column = cuboid.getColumns().get(i);
            int colLength = colIO.getColumnLength(column);
            byte[] value = values[i];
            if (value == null) {
                fillColumnValue(column, colLength, null, 0, bytes, offset);
            } else {
                fillColumnValue(column, colLength, value, value.length, bytes, offset);
            }
            offset += colLength;
        }

        //fill shard and cuboid
        fillHeader(bytes);

        return bytes;
    }

    protected int fillHeader(byte[] bytes) {
        int offset = 0;

        if (encodeShard) {
            short cuboidShardNum = cubeSeg.getCuboidShardNum(cuboid.getId());
            short shardOffset = ShardingHash.getShard(bytes, RowConstants.ROWKEY_HEADER_LEN, bytes.length - RowConstants.ROWKEY_HEADER_LEN, cuboidShardNum);
            short finalShard = ShardingHash.normalize(cubeSeg.getCuboidBaseShard(cuboid.getId()), shardOffset, cubeSeg.getTotalShards());
            BytesUtil.writeShort(finalShard, bytes, offset, RowConstants.ROWKEY_SHARDID_LEN);
        } else {
            BytesUtil.writeShort((short) 0, bytes, offset, RowConstants.ROWKEY_SHARDID_LEN);
        }
        offset += RowConstants.ROWKEY_SHARDID_LEN;

        System.arraycopy(cuboid.getBytes(), 0, bytes, offset, RowConstants.ROWKEY_CUBOIDID_LEN);
        offset += RowConstants.ROWKEY_CUBOIDID_LEN;

        if (this.headerLength != offset) {
            throw new IllegalStateException("Expected header length is " + headerLength + ". But the offset is " + offset);
        }
        
        return offset;
    }

    protected void fillColumnValue(TblColRef column, int columnLen, byte[] value, int valueLen, byte[] outputValue, int outputValueOffset) {
        // special null value case
        if (value == null) {
            byte[] valueBytes = defaultValue(columnLen);
            System.arraycopy(valueBytes, 0, outputValue, outputValueOffset, columnLen);
            return;
        }

        colIO.writeColumn(column, value, valueLen, this.blankByte, outputValue, outputValueOffset);
    }

    protected byte[] defaultValue(int length) {
        byte[] values = new byte[length];
        Arrays.fill(values, this.blankByte);
        return values;
    }

}

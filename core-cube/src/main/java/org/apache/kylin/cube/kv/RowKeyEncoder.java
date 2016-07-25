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
import java.util.Set;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Preconditions;

public class RowKeyEncoder extends AbstractRowKeyEncoder {

    private int bodyLength = 0;
    private RowKeyColumnIO colIO;

    protected boolean enableSharding;
    private int UHCOffset = -1;//it's a offset to the beginning of body
    private int UHCLength = -1;

    public RowKeyEncoder(CubeSegment cubeSeg, Cuboid cuboid) {
        super(cubeSeg, cuboid);
        enableSharding = cubeSeg.isEnableSharding();
        Set<TblColRef> shardByColumns = cubeSeg.getShardByColumns();
        if (shardByColumns.size() > 1) {
            throw new IllegalStateException("Does not support multiple UHC now");
        }
        colIO = new RowKeyColumnIO(cubeSeg.getDimensionEncodingMap());
        for (TblColRef column : cuboid.getColumns()) {
            if (shardByColumns.contains(column)) {
                UHCOffset = bodyLength;
                UHCLength = colIO.getColumnLength(column);
            }
            bodyLength += colIO.getColumnLength(column);
        }
    }

    public int getHeaderLength() {
        return cubeSeg.getRowKeyPreambleSize();
    }

    public int getBytesLength() {
        return getHeaderLength() + bodyLength;
    }

    protected short calculateShard(byte[] key) {
        if (enableSharding) {
            int shardSeedOffset = UHCOffset == -1 ? 0 : UHCOffset;
            int shardSeedLength = UHCLength == -1 ? bodyLength : UHCLength;
            short cuboidShardNum = cubeSeg.getCuboidShardNum(cuboid.getId());
            short shardOffset = ShardingHash.getShard(key, RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN + shardSeedOffset, shardSeedLength, cuboidShardNum);
            return ShardingHash.normalize(cubeSeg.getCuboidBaseShard(cuboid.getId()), shardOffset, cubeSeg.getTotalShards(cuboid.getId()));
        } else {
            throw new RuntimeException("If enableSharding false, you should never calculate shard");
        }
    }

    public int getColumnLength(TblColRef col) {
        return colIO.getColumnLength(col);
    }

    @Override
    public byte[] createBuf() {
        return new byte[this.getBytesLength()];
    }

    @Override
    public void encode(GTRecord record, ImmutableBitSet keyColumns, byte[] buf) {
        ByteArray byteArray = new ByteArray(buf, getHeaderLength(), 0);

        encodeDims(record, keyColumns, byteArray, defaultValue());

        //fill shard and cuboid
        fillHeader(buf);
    }

    //ByteArray representing dimension does not have extra header
    public void encodeDims(GTRecord record, ImmutableBitSet selectedCols, ByteArray buf, byte defaultValue) {
        int pos = 0;
        for (int i = 0; i < selectedCols.trueBitCount(); i++) {
            int c = selectedCols.trueBitAt(i);
            ByteArray columnC = record.get(c);
            if (columnC.array() != null) {
                System.arraycopy(record.get(c).array(), columnC.offset(), buf.array(), buf.offset() + pos, columnC.length());
                pos += columnC.length();
            } else {
                int maxLength = record.getInfo().getCodeSystem().maxCodeLength(c);
                Arrays.fill(buf.array(), buf.offset() + pos, buf.offset() + pos + maxLength, defaultValue);
                pos += maxLength;
            }
        }
        buf.setLength(pos);
    }

    @Override
    public void encode(ByteArray bodyBytes, ByteArray outputBuf) {
        Preconditions.checkState(bodyBytes.length() == bodyLength);
        Preconditions.checkState(bodyBytes.length() + getHeaderLength() == outputBuf.length(), //
                "bodybytes length: " + bodyBytes.length() + " outputBuf length: " + outputBuf.length() + " header length: " + getHeaderLength());
        System.arraycopy(bodyBytes.array(), bodyBytes.offset(), outputBuf.array(), getHeaderLength(), bodyLength);

        //fill shard and cuboid
        fillHeader(outputBuf.array());
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
        byte[] bytes = new byte[this.getBytesLength()];
        int offset = getHeaderLength();

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

    protected void fillHeader(byte[] bytes) {
        int offset = 0;

        if (enableSharding) {
            short shard = calculateShard(bytes);
            BytesUtil.writeShort(shard, bytes, offset, RowConstants.ROWKEY_SHARDID_LEN);
            offset += RowConstants.ROWKEY_SHARDID_LEN;
        }

        System.arraycopy(cuboid.getBytes(), 0, bytes, offset, RowConstants.ROWKEY_CUBOIDID_LEN);
        //offset += RowConstants.ROWKEY_CUBOIDID_LEN;
        //return offset;
    }

    protected void fillColumnValue(TblColRef column, int columnLen, byte[] value, int valueLen, byte[] outputValue, int outputValueOffset) {
        // special null value case
        if (value == null) {
            Arrays.fill(outputValue, outputValueOffset, outputValueOffset + columnLen, defaultValue());
            return;
        }

        colIO.writeColumn(column, value, valueLen, 0, this.blankByte, outputValue, outputValueOffset);
    }

    protected byte defaultValue() {
        return this.blankByte;
    }

}

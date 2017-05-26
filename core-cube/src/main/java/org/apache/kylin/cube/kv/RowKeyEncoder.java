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

import com.google.common.base.Preconditions;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.metadata.model.TblColRef;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RowKeyEncoder extends AbstractRowKeyEncoder implements java.io.Serializable {

    private int bodyLength = 0;
    private RowKeyColumnIO colIO;

    protected boolean enableSharding;
    private int uhcOffset = -1;//it's a offset to the beginning of body
    private int uhcLength = -1;
    private int headerLength;

    public RowKeyEncoder(CubeSegment cubeSeg, Cuboid cuboid) {
        super(cubeSeg, cuboid);
        enableSharding = cubeSeg.isEnableSharding();
        headerLength = cubeSeg.getRowKeyPreambleSize();
        Set<TblColRef> shardByColumns = cubeSeg.getCubeDesc().getShardByColumns();
        if (shardByColumns.size() > 1) {
            throw new IllegalStateException("Does not support multiple UHC now");
        }
        colIO = new RowKeyColumnIO(cubeSeg.getDimensionEncodingMap());
        for (TblColRef column : cuboid.getColumns()) {
            if (shardByColumns.contains(column)) {
                uhcOffset = bodyLength;
                uhcLength = colIO.getColumnLength(column);
            }
            bodyLength += colIO.getColumnLength(column);
        }
    }

    public int getHeaderLength() {
        return headerLength;
    }

    public int getBytesLength() {
        return getHeaderLength() + bodyLength;
    }

    protected short calculateShard(byte[] key) {
        if (enableSharding) {
            int shardSeedOffset = uhcOffset == -1 ? 0 : uhcOffset;
            int shardSeedLength = uhcLength == -1 ? bodyLength : uhcLength;
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
    private void encodeDims(GTRecord record, ImmutableBitSet selectedCols, ByteArray buf, byte defaultValue) {
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
        List<TblColRef> columns = cuboid.getColumns();
        String[] values = new String[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            values[i] = valueMap.get(columns.get(i));
        }
        return encode(values);
    }

    @Override
    public byte[] encode(String[] values) {
        byte[] bytes = new byte[this.getBytesLength()];
        int offset = getHeaderLength();

        for (int i = 0; i < cuboid.getColumns().size(); i++) {
            TblColRef column = cuboid.getColumns().get(i);
            int colLength = colIO.getColumnLength(column);
            fillColumnValue(column, colLength, values[i], bytes, offset);
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

    protected void fillColumnValue(TblColRef column, int columnLen, String valueStr, byte[] outputValue, int outputValueOffset) {
        // special null value case
        if (valueStr == null) {
            Arrays.fill(outputValue, outputValueOffset, outputValueOffset + columnLen, defaultValue());
            return;
        }

        colIO.writeColumn(column, valueStr, 0, this.blankByte, outputValue, outputValueOffset);
    }

    protected byte defaultValue() {
        return this.blankByte;
    }

}

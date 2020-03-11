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

package org.apache.kylin.storage.hbase.lookup;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.dict.lookup.AbstractLookupRowEncoder;
import org.apache.kylin.metadata.model.TableDesc;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.storage.hbase.lookup.HBaseLookupRowEncoder.HBaseRow;

/**
 * encode/decode original table row to hBase row
 * 
 */
public class HBaseLookupRowEncoder extends AbstractLookupRowEncoder<HBaseRow> {
    public static final String CF_STRING = "F";
    public static final byte[] CF = Bytes.toBytes(CF_STRING);

    private int shardNum;

    public HBaseLookupRowEncoder(TableDesc tableDesc, String[] keyColumns, int shardNum) {
        super(tableDesc, keyColumns);
        this.shardNum = shardNum;
    }

    @Override
    public HBaseRow encode(String[] row) {
        String[] keys = getKeyData(row);
        String[] values = getValueData(row);
        byte[] rowKey = encodeRowKey(keys);
        NavigableMap<byte[], byte[]> qualifierValMap = Maps
                .newTreeMap(org.apache.hadoop.hbase.util.Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < values.length; i++) {
            byte[] qualifier = Bytes.toBytes(String.valueOf(valueIndexes[i]));
            byte[] byteValue = toBytes(values[i]);
            qualifierValMap.put(qualifier, byteValue);
        }
        return new HBaseRow(rowKey, qualifierValMap);
    }

    @Override
    public String[] decode(HBaseRow hBaseRow) {
        if (hBaseRow == null) {
            return null;
        }
        String[] result = new String[columnsNum];
        fillKeys(hBaseRow.rowKey, result);
        fillValues(hBaseRow.qualifierValMap, result);

        return result;
    }

    public byte[] encodeRowKey(String[] keys) {
        keyByteBuffer.clear();
        for (String key : keys) {
            if (key == null) {
                throw new IllegalArgumentException("key cannot be null:" + Arrays.toString(keys));
            }
            byte[] byteKey = Bytes.toBytes(key);
            keyByteBuffer.putShort((short) byteKey.length);
            keyByteBuffer.put(byteKey);
        }
        byte[] result = new byte[RowConstants.ROWKEY_SHARDID_LEN + keyByteBuffer.position()];
        System.arraycopy(keyByteBuffer.array(), 0, result, RowConstants.ROWKEY_SHARDID_LEN, keyByteBuffer.position());
        short shard = ShardingHash.getShard(result, RowConstants.ROWKEY_SHARDID_LEN, result.length, shardNum);
        BytesUtil.writeShort(shard, result, 0, RowConstants.ROWKEY_SHARDID_LEN);
        return result;
    }

    private void fillKeys(byte[] rowKey, String[] result) {
        int keyNum = keyIndexes.length;
        ByteBuffer byteBuffer = ByteBuffer.wrap(rowKey);
        byteBuffer.getShort(); // read shard
        for (int i = 0; i < keyNum; i++) {
            short keyLen = byteBuffer.getShort();
            byte[] keyBytes = new byte[keyLen];
            byteBuffer.get(keyBytes);
            result[keyIndexes[i]] = Bytes.toString(keyBytes);
        }
    }

    private void fillValues(Map<byte[], byte[]> qualifierValMap, String[] result) {
        for (Entry<byte[], byte[]> qualifierValEntry : qualifierValMap.entrySet()) {
            byte[] qualifier = qualifierValEntry.getKey();
            byte[] value = qualifierValEntry.getValue();
            int valIdx = Integer.parseInt(Bytes.toString(qualifier));
            result[valIdx] = fromBytes(value);
        }
    }

    public static class HBaseRow {
        private byte[] rowKey;
        private NavigableMap<byte[], byte[]> qualifierValMap;

        public HBaseRow(byte[] rowKey, NavigableMap<byte[], byte[]> qualifierValMap) {
            this.rowKey = rowKey;
            this.qualifierValMap = qualifierValMap;
        }

        public byte[] getRowKey() {
            return rowKey;
        }

        public NavigableMap<byte[], byte[]> getQualifierValMap() {
            return qualifierValMap;
        }
    }
}

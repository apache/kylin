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

package org.apache.kylin.storage.hbase.cube.v2;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Pair;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class RawScan {

    public byte[] startKey;
    public byte[] endKey;
    public List<Pair<byte[], byte[]>> hbaseColumns;//only contain interested columns
    public List<Pair<byte[], byte[]>> fuzzyKeys;
    public int hbaseCaching;
    public int hbaseMaxResultSize;

    public RawScan(byte[] startKey, byte[] endKey, List<Pair<byte[], byte[]>> hbaseColumns, //
            List<Pair<byte[], byte[]>> fuzzyKeys, int hbaseCaching, int hbaseMaxResultSize) {

        this.startKey = startKey;
        this.endKey = endKey;
        this.hbaseColumns = hbaseColumns;
        this.fuzzyKeys = fuzzyKeys;
        this.hbaseCaching = hbaseCaching;
        this.hbaseMaxResultSize = hbaseMaxResultSize;
    }

    public RawScan(RawScan other) {

        this.startKey = other.startKey;
        this.endKey = other.endKey;
        this.hbaseColumns = other.hbaseColumns;
        this.fuzzyKeys = other.fuzzyKeys;
        this.hbaseCaching = other.hbaseCaching;
        this.hbaseMaxResultSize = other.hbaseMaxResultSize;
    }

    public String getStartKeyAsString() {
        return BytesUtil.toHex(this.startKey);
    }

    public String getEndKeyAsString() {
        return BytesUtil.toHex(this.endKey);
    }

    public String getFuzzyKeyAsString() {
        StringBuilder buf = new StringBuilder();
        for (Pair<byte[], byte[]> fuzzyKey : this.fuzzyKeys) {
            buf.append(BytesUtil.toHex(fuzzyKey.getFirst()));
            buf.append(" ");
            buf.append(BytesUtil.toHex(fuzzyKey.getSecond()));
            buf.append(";");
        }
        return buf.toString();
    }

    public static final BytesSerializer<RawScan> serializer = new BytesSerializer<RawScan>() {
        @Override
        public void serialize(RawScan value, ByteBuffer out) {
            BytesUtil.writeByteArray(value.startKey, out);
            BytesUtil.writeByteArray(value.endKey, out);
            BytesUtil.writeVInt(value.hbaseColumns.size(), out);
            for (Pair<byte[], byte[]> hbaseColumn : value.hbaseColumns) {
                BytesUtil.writeByteArray(hbaseColumn.getFirst(), out);
                BytesUtil.writeByteArray(hbaseColumn.getSecond(), out);
            }
            BytesUtil.writeVInt(value.fuzzyKeys.size(), out);
            for (Pair<byte[], byte[]> fuzzyKey : value.fuzzyKeys) {
                BytesUtil.writeByteArray(fuzzyKey.getFirst(), out);
                BytesUtil.writeByteArray(fuzzyKey.getSecond(), out);
            }
            BytesUtil.writeVInt(value.hbaseCaching, out);
            BytesUtil.writeVInt(value.hbaseMaxResultSize, out);
        }

        @Override
        public RawScan deserialize(ByteBuffer in) {
            byte[] sStartKey = BytesUtil.readByteArray(in);
            byte[] sEndKey = BytesUtil.readByteArray(in);
            int hbaseColumnsSize = BytesUtil.readVInt(in);
            List<Pair<byte[], byte[]>> sHbaseCoumns = Lists.newArrayListWithCapacity(hbaseColumnsSize);
            for (int i = 0; i < hbaseColumnsSize; i++) {
                byte[] a = BytesUtil.readByteArray(in);
                byte[] b = BytesUtil.readByteArray(in);
                sHbaseCoumns.add(Pair.newPair(a, b));
            }

            int fuzzyKeysSize = BytesUtil.readVInt(in);
            List<Pair<byte[], byte[]>> sFuzzyKeys = Lists.newArrayListWithCapacity(fuzzyKeysSize);
            for (int i = 0; i < fuzzyKeysSize; i++) {
                byte[] a = BytesUtil.readByteArray(in);
                byte[] b = BytesUtil.readByteArray(in);
                sFuzzyKeys.add(Pair.newPair(a, b));
            }
            int sHBaseCaching = BytesUtil.readVInt(in);
            int sHBaseMaxResultSize = BytesUtil.readVInt(in);
            return new RawScan(sStartKey, sEndKey, sHbaseCoumns, sFuzzyKeys, sHBaseCaching, sHBaseMaxResultSize);
        }
    };

}

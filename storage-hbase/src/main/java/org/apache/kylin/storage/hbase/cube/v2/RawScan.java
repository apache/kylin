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

import java.util.List;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Pair;

public class RawScan {

    public byte[] startKey;
    public byte[] endKey;
    public List<Pair<byte[], byte[]>> hbaseColumns;//only contain interested columns
    public List<Pair<byte[], byte[]>> fuzzyKey;
    public int hbaseCaching;
    public int hbaseMaxResultSize;

    public RawScan(byte[] startKey, byte[] endKey, List<Pair<byte[], byte[]>> hbaseColumns, //
            List<Pair<byte[], byte[]>> fuzzyKey, int hbaseCaching, int hbaseMaxResultSize) {

        this.startKey = startKey;
        this.endKey = endKey;
        this.hbaseColumns = hbaseColumns;
        this.fuzzyKey = fuzzyKey;
        this.hbaseCaching = hbaseCaching;
        this.hbaseMaxResultSize = hbaseMaxResultSize;
    }

    public String getStartKeyAsString() {
        return BytesUtil.toHex(this.startKey);
    }

    public String getEndKeyAsString() {
        return BytesUtil.toHex(this.endKey);
    }

    public String getFuzzyKeyAsString() {
        StringBuilder buf = new StringBuilder();
        for (Pair<byte[], byte[]> fuzzyKey : this.fuzzyKey) {
            buf.append(BytesUtil.toHex(fuzzyKey.getFirst()));
            buf.append(" ");
            buf.append(BytesUtil.toHex(fuzzyKey.getSecond()));
            buf.append(System.lineSeparator());
        }
        return buf.toString();
    }

}

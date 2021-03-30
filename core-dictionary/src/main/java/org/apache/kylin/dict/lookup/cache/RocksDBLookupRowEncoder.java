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

package org.apache.kylin.dict.lookup.cache;

import org.apache.kylin.dict.lookup.AbstractLookupRowEncoder;
import org.apache.kylin.dict.lookup.cache.RocksDBLookupRowEncoder.KV;
import org.apache.kylin.metadata.model.TableDesc;

/**
 * encode/decode original table row to rocksDB KV
 * 
 */
public class RocksDBLookupRowEncoder extends AbstractLookupRowEncoder<KV>{

    public RocksDBLookupRowEncoder(TableDesc tableDesc, String[] keyColumns) {
        super(tableDesc, keyColumns);
    }

    public KV encode(String[] row) {
        String[] keys = getKeyData(row);
        String[] values = getValueData(row);
        byte[] encodeKey = encodeStringsWithLenPfx(keys, false);
        byte[] encodeValue = encodeStringsWithLenPfx(values, true);

        return new KV(encodeKey, encodeValue);
    }

    public String[] decode(KV kv) {
        String[] result = new String[columnsNum];

        decodeFromLenPfxBytes(kv.key, keyIndexes, result);
        decodeFromLenPfxBytes(kv.value, valueIndexes, result);

        return result;
    }

    public static class KV {
        private byte[] key;
        private byte[] value;

        public KV(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public byte[] getKey() {
            return key;
        }

        public byte[] getValue() {
            return value;
        }
    }
}

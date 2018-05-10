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

package org.apache.kylin.dict.lookup;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.metadata.model.TableDesc;

/**
 * Abstract encoder/decoder
 * 
 */
abstract public class AbstractLookupRowEncoder<R> {
    protected ByteBuffer keyByteBuffer = ByteBuffer.allocate(1024 * 1024);

    protected int columnsNum;
    protected int[] keyIndexes;
    protected int[] valueIndexes;

    public AbstractLookupRowEncoder(TableDesc tableDesc, String[] keyColumns) {
        this.columnsNum = tableDesc.getColumns().length;
        this.keyIndexes = new int[keyColumns.length];
        this.valueIndexes = new int[columnsNum - keyColumns.length];
        int keyIdx = 0;
        int valIdx = 0;
        for (int i = 0; i < columnsNum; i++) {
            boolean isKeyColumn = false;
            for (String keyColumn : keyColumns) {
                if (keyColumn.equals(tableDesc.getColumns()[i].getName())) {
                    isKeyColumn = true;
                    break;
                }
            }
            if (isKeyColumn) {
                keyIndexes[keyIdx] = i;
                keyIdx++;
            } else {
                valueIndexes[valIdx] = i;
                valIdx++;
            }
        }
    }

    abstract public R encode(String[] row);

    abstract public String[] decode(R result);

    public String[] getKeyData(String[] row) {
        return extractColValues(row, keyIndexes);
    }

    public String[] getValueData(String[] row) {
        return extractColValues(row, valueIndexes);
    }

    public byte[] encodeStringsWithLenPfx(String[] keys, boolean allowNull) {
        keyByteBuffer.clear();
        for (String key : keys) {
            if (key == null && !allowNull) {
                throw new IllegalArgumentException("key cannot be null:" + Arrays.toString(keys));
            }
            byte[] byteKey = toBytes(key);
            keyByteBuffer.putShort((short) byteKey.length);
            keyByteBuffer.put(byteKey);
        }
        byte[] result = new byte[keyByteBuffer.position()];
        System.arraycopy(keyByteBuffer.array(), 0, result, 0, keyByteBuffer.position());
        return result;
    }

    protected void decodeFromLenPfxBytes(byte[] rowKey, int[] valueIdx, String[] result) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(rowKey);
        for (int i = 0; i < valueIdx.length; i++) {
            short keyLen = byteBuffer.getShort();
            byte[] keyBytes = new byte[keyLen];
            byteBuffer.get(keyBytes);
            result[valueIdx[i]] = fromBytes(keyBytes);
        }
    }

    protected String[] extractColValues(String[] row, int[] indexes) {
        String[] result = new String[indexes.length];
        int i = 0;
        for (int idx : indexes) {
            result[i++] = row[idx];
        }
        return result;
    }

    protected byte[] toBytes(String str) {
        if (str == null) {
            return new byte[] { DimensionEncoding.NULL };
        }
        return Bytes.toBytes(str);
    }

    protected String fromBytes(byte[] bytes) {
        if (DimensionEncoding.isNull(bytes, 0, bytes.length)) {
            return null;
        }
        return Bytes.toString(bytes);
    }
}

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

package org.apache.kylin.stream.core.storage.columnar.invertindex;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * not thread safe
 */
public class FixLenColInvertIndexWriter extends ColInvertIndexWriter {
    private int valueLenInBytes;

    private int rows;
    private Map<byte[], MutableRoaringBitmap> valueBitmaps = new TreeMap<>(new Bytes.ByteArrayComparator());

    public FixLenColInvertIndexWriter(String columnName, int valueLenInBytes) {
        super(columnName);
        this.valueLenInBytes = valueLenInBytes;
    }

    @Override
    public void addValue(byte[] value) {
        if (value.length != valueLenInBytes) {
            throw new IllegalArgumentException("the value:" + Bytes.toHex(value) + " is not valid.");
        }
        MutableRoaringBitmap bitmap = valueBitmaps.get(value);
        if (bitmap == null) {
            bitmap = new MutableRoaringBitmap();
            valueBitmaps.put(value, bitmap);
        }
        bitmap.add(++rows);
    }

    @Override
    public void write(OutputStream out) throws IOException {
        int cardinality = valueBitmaps.size();
        int footLen = II_DICT_TYPE_SORT_VAL_FOOT_LEN;
        int dictAndFootLen = cardinality * (valueLenInBytes + 4) + footLen;

        DataOutputStream bitmapOut = new DataOutputStream(out);
        ByteBuffer dictBuffer = ByteBuffer.allocate(dictAndFootLen);
        int offset = 0;
        for (Map.Entry<byte[], MutableRoaringBitmap> bitmapEntry : valueBitmaps.entrySet()) {
            byte[] colValue = bitmapEntry.getKey();
            MutableRoaringBitmap bitmap = bitmapEntry.getValue();
            bitmap.runOptimize();
            int bitmapSize = bitmap.serializedSizeInBytes();
            bitmap.serialize(bitmapOut);

            dictBuffer.put(colValue);
            dictBuffer.putInt(offset);
            offset += bitmapSize;
        }

        dictBuffer.putInt(cardinality);
        dictBuffer.putInt(valueLenInBytes);
        dictBuffer.putInt(II_DICT_TYPE_SORT_VAL); // type
        out.write(dictBuffer.array(), 0, dictBuffer.position());

        out.flush();
    }

}

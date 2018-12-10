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

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.dimension.DimensionEncoding;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * not thread safe
 */
public class SeqColInvertIndexWriter extends ColInvertIndexWriter {
    private int minColVal;
    private int maxColVal;

    private MutableRoaringBitmap[] valueBitmaps;
    private MutableRoaringBitmap nullValueBitmap;

    private int rows;

    public SeqColInvertIndexWriter(String columnName, int minColVal, int maxColVal) {
        super(columnName);
        this.minColVal = minColVal;
        this.maxColVal = maxColVal;
        this.valueBitmaps = new MutableRoaringBitmap[maxColVal - minColVal + 1];
    }

    @Override
    public void addValue(byte[] value) {
        if (value == null || DimensionEncoding.isNull(value, 0, value.length)) {
            if (nullValueBitmap == null) {
                nullValueBitmap = new MutableRoaringBitmap();
            }
            nullValueBitmap.add(++rows);
        } else {
            int intVal = Bytes.readAsInt(value, 0, value.length);
            if (intVal < minColVal || intVal > maxColVal) {
                throw new IllegalArgumentException("the value:" + intVal + " is not in (" + minColVal + "," + maxColVal
                        + ")");
            }
            int idx = intVal - minColVal;
            MutableRoaringBitmap bitmap = valueBitmaps[idx];
            if (bitmap == null) {
                bitmap = new MutableRoaringBitmap();
                valueBitmaps[idx] = bitmap;
            }
            bitmap.add(++rows);
        }
    }

    @Override
    public void write(OutputStream out) throws IOException {
        int cardinality = valueBitmaps.length;
        if (nullValueBitmap != null) {
            cardinality++;
        }
        int dictionaryPartLen = cardinality * 4;

        DataOutputStream bitmapOut = new DataOutputStream(out);
        int footAndDictLen = II_DICT_TYPE_SEQ_FOOT_LEN + dictionaryPartLen;
        ByteBuffer footBuffer = ByteBuffer.allocate(footAndDictLen);

        int offset = 0;
        for (MutableRoaringBitmap bitmap : valueBitmaps) {
            bitmap.runOptimize();
            int bitmapSize = bitmap.serializedSizeInBytes();
            bitmap.serialize(bitmapOut);

            footBuffer.putInt(offset);
            offset += bitmapSize;
        }
        if (nullValueBitmap != null) {
            nullValueBitmap.serialize(bitmapOut);
            footBuffer.putInt(offset);
        }

        footBuffer.putInt(cardinality); // cardinality
        footBuffer.putInt(minColVal);
        footBuffer.putInt(maxColVal);
        footBuffer.putInt(II_DICT_TYPE_SEQ);// type
        if (nullValueBitmap != null) {
            footBuffer.put((byte) 1);
        } else {
            footBuffer.put((byte) 0);
        }
        out.write(footBuffer.array(), 0, footBuffer.position());

        out.flush();
    }
}

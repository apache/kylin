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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.dimension.DimensionEncoding;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

public class ColInvertIndexSearcher {

    private ByteBuffer bitmapBuffer;

    private int bitmapStartOffset;

    private IOffsetDictionary offsetDictionary;

    public ColInvertIndexSearcher() {
    }

    /**
     * Load the Inverted index bitmap.
     *
     * @param buffer
     * @return
     * @throws IOException
     */
    public static ColInvertIndexSearcher load(ByteBuffer buffer) throws IOException {
        ColInvertIndexSearcher result = new ColInvertIndexSearcher();
        result.bitmapStartOffset = buffer.position();
        int type = buffer.getInt(buffer.limit() - 4);
        if (type == ColInvertIndexWriter.II_DICT_TYPE_SEQ) {
            result.offsetDictionary = new SeqOffsetDictionary(buffer);
        } else {
            result.offsetDictionary = new SortValueOffsetDictionary(buffer);
        }
        result.bitmapBuffer = buffer;
        return result;
    }

    public ImmutableRoaringBitmap searchValue(byte[] value) {
        int offset = offsetDictionary.getBitMapOffset(value);
        if (offset == -1) {
            return null;
        }
        ByteBuffer usedBuffer = bitmapBuffer.asReadOnlyBuffer();
        usedBuffer.position(bitmapStartOffset + offset);
        ImmutableRoaringBitmap bitmap = new ImmutableRoaringBitmap(usedBuffer);

        return bitmap;
    }

    public interface IOffsetDictionary {
        int getBitMapOffset(byte[] value);
    }

    public static class SeqOffsetDictionary implements IOffsetDictionary {
        private int minVal;
        private int maxVal;
        private boolean hasNullVal = false;
        private ByteBuffer offsetBuffer;
        private int dictStartOffset;
        private int cardinality;

        SeqOffsetDictionary(ByteBuffer byteBuffer) {
            this.offsetBuffer = byteBuffer;

            byteBuffer.position(byteBuffer.limit() - ColInvertIndexWriter.II_DICT_TYPE_SEQ_FOOT_LEN);
            this.cardinality = byteBuffer.getInt();
            this.minVal = byteBuffer.getInt();
            this.maxVal = byteBuffer.getInt();
            this.dictStartOffset = byteBuffer.limit() - ColInvertIndexWriter.II_DICT_TYPE_SEQ_FOOT_LEN
                    - (cardinality << 2);
            byte nullByte = byteBuffer.get();
            if (nullByte != 0) {
                hasNullVal = true;
            }
        }

        @Override
        public int getBitMapOffset(byte[] value) {
            if (value == null || DimensionEncoding.isNull(value, 0, value.length)) {
                if (!hasNullVal) {
                    return -1;
                } else {
                    return offsetBuffer.getInt(dictStartOffset + ((cardinality - 1) << 2));
                }
            } else {
                int intVal = Bytes.readAsInt(value, 0, value.length);
                if (intVal < minVal || intVal > maxVal) {
                    return -1;
                }
                int idx = intVal - minVal;
                return offsetBuffer.getInt(dictStartOffset + (idx << 2));
            }
        }
    }

    public static class SortValueOffsetDictionary implements IOffsetDictionary {
        private int valueLen;
        private ByteBuffer offsetBuffer;
        private int cardinality;
        private int dictStartOffset;

        SortValueOffsetDictionary(ByteBuffer byteBuffer) {
            this.offsetBuffer = byteBuffer;
            byteBuffer.position(byteBuffer.limit() - ColInvertIndexWriter.II_DICT_TYPE_SORT_VAL_FOOT_LEN);
            this.cardinality = byteBuffer.getInt();
            this.valueLen = byteBuffer.getInt();
            this.dictStartOffset = byteBuffer.limit() - ColInvertIndexWriter.II_DICT_TYPE_SORT_VAL_FOOT_LEN
                    - (cardinality * (valueLen + 4));
        }

        @Override
        public int getBitMapOffset(byte[] value) {
            if (value == null) {
                value = getNullValue();
            }
            return binarySearch(value);
        }

        private int binarySearch(byte[] value) {
            int low = 0;
            int high = cardinality - 1;
            byte[] currVal = new byte[valueLen];
            while (low <= high) {
                int mid = (low + high) >>> 1;
                offsetBuffer.position(dictStartOffset + mid * (valueLen + 4));
                offsetBuffer.get(currVal);
                int cmp = Bytes.compareTo(value, currVal);
                if (cmp == 0) {
                    return offsetBuffer.getInt();
                } else if (cmp < 0) {
                    high = mid - 1;
                } else {
                    low = mid + 1;
                }
            }
            return -1;
        }

        private byte[] getNullValue() {
            byte[] result = new byte[valueLen];
            for (int i = 0; i < valueLen; i++) {
                result[i] = DimensionEncoding.NULL;
            }
            return result;
        }

    }
}

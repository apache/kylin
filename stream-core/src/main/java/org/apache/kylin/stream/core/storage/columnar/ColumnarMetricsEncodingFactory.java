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

package org.apache.kylin.stream.core.storage.columnar;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.datatype.DoubleSerializer;
import org.apache.kylin.metadata.datatype.Long8Serializer;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class ColumnarMetricsEncodingFactory {
    private static Set<String> longEncodingTypes = Sets.newHashSet();
    private static Set<String> doubleEncodingTypes = Sets.newHashSet();
    static {
        longEncodingTypes.add("bigint");
        longEncodingTypes.add("long");
        longEncodingTypes.add("integer");
        longEncodingTypes.add("int");
        longEncodingTypes.add("tinyint");
        longEncodingTypes.add("smallint");

        doubleEncodingTypes.add("double");
        doubleEncodingTypes.add("float");
    }

    public static ColumnarMetricsEncoding create(DataType dataType) {
        if (longEncodingTypes.contains(dataType.getName())) {
            return new ColumnarLongMetricsEncoding(dataType);
        }
        if (doubleEncodingTypes.contains(dataType.getName())) {
            return new ColumnarDoubleMetricsEncoding(dataType);
        }
        return new ColumnarComplexMetricsEncoding(dataType);
    }

    public static class ColumnarLongMetricsEncoding extends ColumnarMetricsEncoding<Long> {
        public ColumnarLongMetricsEncoding(DataType dataType) {
            super(dataType);
        }

        @Override
        public boolean isFixLength() {
            return true;
        }

        @Override
        public int getFixLength() {
            return 8;
        }

        @Override
        public DataTypeSerializer<Long> asDataTypeSerializer() {
            return new Long8Serializer(dataType);
        }
    }

    public static class ColumnarFixLenLongMetricsEncoding extends ColumnarMetricsEncoding<Long> {
        private int fixLength;

        public ColumnarFixLenLongMetricsEncoding(DataType dataType, int fixLength) {
            super(dataType);
            this.fixLength = fixLength;
        }

        @Override
        public boolean isFixLength() {
            return true;
        }

        @Override
        public int getFixLength() {
            return fixLength;
        }

        @Override
        public DataTypeSerializer<Long> asDataTypeSerializer() {
            return new DataTypeSerializer<Long>() {
                @Override
                public int peekLength(ByteBuffer in) {
                    return fixLength;
                }

                @Override
                public int maxLength() {
                    return fixLength;
                }

                @Override
                public int getStorageBytesEstimate() {
                    return fixLength;
                }

                @Override
                public void serialize(Long value, ByteBuffer out) {
                    long longV = value;
                    for (int i = 0; i < fixLength; i++) {
                        out.put((byte) longV);
                        longV >>>= 8;
                    }
                }

                @Override
                public Long deserialize(ByteBuffer in) {
                    long integer = 0;
                    int mask = 0xff;
                    int shift = 0;
                    for (int i = 0; i < fixLength; i++) {
                        integer |= (in.get() << shift) & mask;
                        mask = mask << 8;
                        shift += 8;
                    }
                    return integer;
                }
            };
        }
    }

    public static class ColumnarDoubleMetricsEncoding extends ColumnarMetricsEncoding<Double> {

        public ColumnarDoubleMetricsEncoding(DataType dataType) {
            super(dataType);
        }

        @Override
        public boolean isFixLength() {
            return true;
        }

        @Override
        public int getFixLength() {
            return 8;
        }

        @Override
        public DataTypeSerializer<Double> asDataTypeSerializer() {
            return new DoubleSerializer(dataType);
        }
    }

    public static class ColumnarComplexMetricsEncoding extends ColumnarMetricsEncoding {

        public ColumnarComplexMetricsEncoding(DataType dataType) {
            super(dataType);
        }

        @Override
        public boolean isFixLength() {
            return false;
        }

        @Override
        public int getFixLength() {
            return -1;
        }

        @Override
        public DataTypeSerializer asDataTypeSerializer() {
            return DataTypeSerializer.create(dataType);
        }
    }
}

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
import java.nio.charset.Charset;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DictionaryDimEnc.DictionarySerializer;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

public class ColumnarRecordCodec {
    private DimensionEncoding[] dimEncodings;
    private DataTypeSerializer[] dimSerializers;
    private DataTypeSerializer[] metricsSerializers;

    public ColumnarRecordCodec(DimensionEncoding[] dimEncodings, ColumnarMetricsEncoding[] metricsEncodings) {
        this.dimEncodings = dimEncodings;
        this.dimSerializers = new DataTypeSerializer[dimEncodings.length];
        for (int i = 0; i < dimEncodings.length; i++) {
            dimSerializers[i] = dimEncodings[i].asDataTypeSerializer();
        }
        this.metricsSerializers = new DataTypeSerializer[metricsEncodings.length];
        for (int i = 0; i < metricsEncodings.length; i++) {
            metricsSerializers[i] = metricsEncodings[i].asDataTypeSerializer();
        }
    }

    public Object decodeMetrics(int i, byte[] metricsVal) {
        return metricsSerializers[i].deserialize(ByteBuffer.wrap(metricsVal));
    }

    public String decodeDimension(int i, byte[] dimVal) {
        return dimEncodings[i].decode(dimVal, 0, dimVal.length);
    }

    public void encodeDimension(int col, Object value, int roundingFlag, ByteBuffer buf) {
        DataTypeSerializer serializer = dimSerializers[col];
        if (serializer instanceof DictionarySerializer) {
            DictionaryDimEnc dictEnc = ((DictionaryDimEnc) dimEncodings[col]);
            if (dictEnc.getRoundingFlag() != roundingFlag) {
                serializer = dictEnc.copy(roundingFlag).asDataTypeSerializer();
            }
            try {
                serializer.serialize(value, buf);
            } catch (IllegalArgumentException ex) {
                IllegalArgumentException rewordEx = new IllegalArgumentException("Column " + col + " value '" + toStringBinary(value) + "' met dictionary error: " + ex.getMessage());
                rewordEx.setStackTrace(ex.getStackTrace());
                throw rewordEx;
            }
        } else {
            if (value instanceof String) {
                // for dimensions; measures are converted by MeasureIngestor before reaching this point
                value = serializer.valueOf((String) value);
            }
            serializer.serialize(value, buf);
        }
    }

    public int getMaxDimLength() {
        int max = 0;
        for (int i = 0; i < dimSerializers.length; i++) {
            max = Math.max(max, dimSerializers[i].maxLength());
        }
        return max;
    }

    public int getMaxMetricsLength() {
        int max = 0;
        for (int i = 0; i < metricsSerializers.length; i++) {
            max = Math.max(max, metricsSerializers[i].maxLength());
        }
        return max;
    }

    public DimensionEncoding[] getDimensionEncodings() {
        return dimEncodings;
    }

    private String toStringBinary(Object value) {
        if (value == null)
            return "Null";
        byte[] bytes;
        bytes = value.toString().getBytes(Charset.forName("UTF-8"));
        return Bytes.toStringBinary(bytes);
    }
}

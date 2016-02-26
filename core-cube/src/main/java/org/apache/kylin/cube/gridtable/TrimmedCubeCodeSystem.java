/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  * 
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  * 
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 * /
 */

package org.apache.kylin.cube.gridtable;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.gridtable.DefaultGTComparator;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.IGTCodeSystem;
import org.apache.kylin.gridtable.IGTComparator;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

import com.google.common.collect.Maps;

/**
 * A limited code system where dimension value ser/des is disabled.
 * Used inside coprocessor only. Because dictionary is not available. 
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TrimmedCubeCodeSystem implements IGTCodeSystem {

    private GTInfo info;

    private DimensionEncoding[] dimEncs;
    private DataTypeSerializer[] serializers;
    private IGTComparator comparator;
    private Map<Integer, Integer> dependentMetricsMap;

    public TrimmedCubeCodeSystem(DimensionEncoding[] dimEncs, Map<Integer, Integer> dependentMetricsMap) {
        this.dimEncs = dimEncs;
        this.comparator = new DefaultGTComparator();
        this.dependentMetricsMap = dependentMetricsMap;
    }

    @Override
    public void init(GTInfo info) {
        this.info = info;

        this.serializers = new DataTypeSerializer[info.getColumnCount()];
        for (int i = 0; i < serializers.length; i++) {
            DimensionEncoding dimEnc = i < dimEncs.length ? dimEncs[i] : null;

            // for dimensions
            if (dimEnc != null) {
                // use trimmed serializer cause no dictionary in coprocessor
                serializers[i] = new TrimmedDimensionSerializer(dimEnc.getLengthOfEncoding());
            }
            // for measures
            else {
                serializers[i] = DataTypeSerializer.create(info.getColumnType(i));
            }
        }
    }

    @Override
    public IGTComparator getComparator() {
        return comparator;
    }

    @Override
    public int codeLength(int col, ByteBuffer buf) {
        return serializers[col].peekLength(buf);
    }

    @Override
    public int maxCodeLength(int col) {
        return serializers[col].maxLength();
    }

    @Override
    public void encodeColumnValue(int col, Object value, ByteBuffer buf) {
        encodeColumnValue(col, value, 0, buf);
    }

    @Override
    public void encodeColumnValue(int col, Object value, int roundingFlag, ByteBuffer buf) {
        DataTypeSerializer serializer = serializers[col];
        serializer.serialize(value, buf);
    }

    @Override
    public Object decodeColumnValue(int col, ByteBuffer buf) {
        return serializers[col].deserialize(buf);
    }

    //TODO: remove duplicate
    @Override
    public MeasureAggregator<?>[] newMetricsAggregators(ImmutableBitSet columns, String[] aggrFunctions) {
        assert columns.trueBitCount() == aggrFunctions.length;

        MeasureAggregator<?>[] result = new MeasureAggregator[aggrFunctions.length];
        for (int i = 0; i < result.length; i++) {
            int col = columns.trueBitAt(i);
            result[i] = MeasureAggregator.create(aggrFunctions[i], info.getColumnType(col));
        }

        // deal with holistic distinct count
        if (dependentMetricsMap != null) {
            for (Integer child : dependentMetricsMap.keySet()) {
                if (columns.get(child)) {
                    Integer parent = dependentMetricsMap.get(child);
                    if (columns.get(parent) == false)
                        throw new IllegalStateException();

                    int childIdx = columns.trueBitIndexOf(child);
                    int parentIdx = columns.trueBitIndexOf(parent);
                    result[childIdx].setDependentAggregator(result[parentIdx]);
                }
            }
        }

        return result;
    }

    public static final BytesSerializer<TrimmedCubeCodeSystem> serializer = new BytesSerializer<TrimmedCubeCodeSystem>() {
        @Override
        public void serialize(TrimmedCubeCodeSystem value, ByteBuffer out) {
            BytesUtil.writeVInt(value.dependentMetricsMap.size(), out);
            for (Map.Entry<Integer, Integer> x : value.dependentMetricsMap.entrySet()) {
                BytesUtil.writeVInt(x.getKey(), out);
                BytesUtil.writeVInt(x.getValue(), out);
            }

            BytesUtil.writeVInt(value.dimEncs.length, out);
            for (int i = 0; i < value.dimEncs.length; i++) {
                DimensionEncoding enc = value.dimEncs[i];
                BytesUtil.writeVInt(enc == null ? 0 : enc.getLengthOfEncoding(), out);
            }
        }

        @Override
        public TrimmedCubeCodeSystem deserialize(ByteBuffer in) {
            Map<Integer, Integer> dependentMetricsMap = Maps.newHashMap();

            int size = BytesUtil.readVInt(in);
            for (int i = 0; i < size; ++i) {
                int key = BytesUtil.readVInt(in);
                int value = BytesUtil.readVInt(in);
                dependentMetricsMap.put(key, value);
            }

            DimensionEncoding[] dimEncs = new DimensionEncoding[BytesUtil.readVInt(in)];
            for (int i = 0; i < dimEncs.length; i++) {
                int fixedLen = BytesUtil.readVInt(in);
                if (fixedLen > 0)
                    dimEncs[i] = new TrimmedDimEnc(fixedLen);
            }

            return new TrimmedCubeCodeSystem(dimEncs, dependentMetricsMap);
        }
    };

    static class TrimmedDimEnc extends DimensionEncoding {
        final int fixedLen;

        TrimmedDimEnc(int fixedLen) {
            this.fixedLen = fixedLen;
        }
        
        @Override
        public int getLengthOfEncoding() {
            return fixedLen;
        }

        @Override
        public void encode(byte[] value, int valueLen, byte[] output, int outputOffset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String decode(byte[] bytes, int offset, int len) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataTypeSerializer<Object> asDataTypeSerializer() {
            throw new UnsupportedOperationException();
        }
    }
    
    static class TrimmedDimensionSerializer extends DataTypeSerializer<Object> {

        final int fixedLen;

        public TrimmedDimensionSerializer(int fixedLen) {
            this.fixedLen = fixedLen;
        }

        @Override
        public int peekLength(ByteBuffer in) {
            return fixedLen;
        }

        @Override
        public int maxLength() {
            return fixedLen;
        }

        @Override
        public int getStorageBytesEstimate() {
            return fixedLen;
        }

        @Override
        public void serialize(Object value, ByteBuffer out) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object deserialize(ByteBuffer in) {
            throw new UnsupportedOperationException();
        }
    }

}

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

package org.apache.kylin.cube.gridtable;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DictionaryDimEnc.DictionarySerializer;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.gridtable.DefaultGTComparator;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.IGTCodeSystem;
import org.apache.kylin.gridtable.IGTComparator;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

/**
 * defines how column values will be encoded to/ decoded from GTRecord 
 * 
 * Cube meta can provide which columns are dictionary encoded (dict encoded dimensions) or fixed length encoded (fixed length dimensions)
 * Metrics columns are more flexible, they will use DataTypeSerializer according to their data type.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class CubeCodeSystem implements IGTCodeSystem {

    GTInfo info;

    DimensionEncoding[] dimEncs;
    DataTypeSerializer[] serializers;
    IGTComparator comparator;
    Map<Integer, Integer> dependentMetricsMap;

    public CubeCodeSystem(DimensionEncoding[] dimEncs) {
        this(dimEncs, Collections.<Integer, Integer> emptyMap());
    }

    public CubeCodeSystem(DimensionEncoding[] dimEncs, Map<Integer, Integer> dependentMetricsMap) {
        this.dimEncs = dimEncs;
        this.comparator = new DefaultGTComparator();
        this.dependentMetricsMap = dependentMetricsMap;
    }

    public TrimmedCubeCodeSystem trimForCoprocessor() {
        return new TrimmedCubeCodeSystem(dimEncs, dependentMetricsMap);
    }

    @Override
    public void init(GTInfo info) {
        this.info = info;

        this.serializers = new DataTypeSerializer[info.getColumnCount()];
        for (int i = 0; i < serializers.length; i++) {
            DimensionEncoding dimEnc = i < dimEncs.length ? dimEncs[i] : null;

            if (dimEnc != null) {
                // for dimensions
                serializers[i] = dimEnc.asDataTypeSerializer();
            } else {
                // for measures
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
    public DimensionEncoding getDimEnc(int col) {
        if (col < dimEncs.length) {
            return dimEncs[col];
        } else {
            return null;
        }
    }

    @Override
    public void encodeColumnValue(int col, Object value, ByteBuffer buf) {
        encodeColumnValue(col, value, 0, buf);
    }

    @Override
    public void encodeColumnValue(int col, Object value, int roundingFlag, ByteBuffer buf) {
        DataTypeSerializer serializer = serializers[col];
        if (serializer instanceof DictionarySerializer) {
            DictionaryDimEnc dictEnc = ((DictionaryDimEnc) dimEncs[col]);
            if (dictEnc.getRoundingFlag() != roundingFlag) {
                serializer = dictEnc.copy(roundingFlag).asDataTypeSerializer();
            }
            serializer.serialize(value, buf);
        } else {
            if (value instanceof String) {
                // for dimensions; measures are converted by MeasureIngestor before reaching this point
                value = serializer.valueOf((String) value);
            }
            serializer.serialize(value, buf);
        }
    }

    @Override
    public Object decodeColumnValue(int col, ByteBuffer buf) {
        return serializers[col].deserialize(buf);
    }

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

}

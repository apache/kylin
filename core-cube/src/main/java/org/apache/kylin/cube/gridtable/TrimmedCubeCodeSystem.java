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
import org.apache.kylin.gridtable.DefaultGTComparator;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.IGTCodeSystem;
import org.apache.kylin.gridtable.IGTComparator;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

import com.google.common.collect.Maps;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class TrimmedCubeCodeSystem implements IGTCodeSystem {

    private Map<Integer, Integer> dependentMetricsMap;
    private Map<Integer, Integer> dictSizes;
    private Map<Integer, Integer> fixedLengthSize;

    private transient GTInfo info;
    private transient DataTypeSerializer[] serializers;
    private transient IGTComparator comparator;

    public TrimmedCubeCodeSystem(Map<Integer, Integer> dependentMetricsMap, Map<Integer, Integer> dictSizes, Map<Integer, Integer> fixedLengthSize) {
        this.dependentMetricsMap = dependentMetricsMap;
        this.dictSizes = dictSizes;
        this.fixedLengthSize = fixedLengthSize;
    }

    @Override
    public void init(GTInfo info) {
        this.info = info;

        serializers = new DataTypeSerializer[info.getColumnCount()];
        for (int i = 0; i < info.getColumnCount(); i++) {
            // dimension with dictionary
            if (dictSizes.get(i) != null) {
                serializers[i] = new CubeCodeSystem.TrimmedDictionarySerializer(dictSizes.get(i));
            }
            // dimension of fixed length
            else if (fixedLengthSize.get(i) != null) {
                serializers[i] = new FixLenSerializer(fixedLengthSize.get(i));
            }
            // metrics
            else {
                serializers[i] = DataTypeSerializer.create(info.getColumnType(i));
            }
        }

        this.comparator = new DefaultGTComparator();
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

        //        if (((value instanceof String) && !(serializer instanceof StringSerializer || serializer instanceof CubeCodeSystem.FixLenSerializer))) {
        //            value = serializer.valueOf((String) value);
        //        }

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

            BytesUtil.writeVInt(value.dictSizes.size(), out);
            for (Map.Entry<Integer, Integer> x : value.dictSizes.entrySet()) {
                BytesUtil.writeVInt(x.getKey(), out);
                BytesUtil.writeVInt(x.getValue(), out);
            }

            BytesUtil.writeVInt(value.fixedLengthSize.size(), out);
            for (Map.Entry<Integer, Integer> x : value.fixedLengthSize.entrySet()) {
                BytesUtil.writeVInt(x.getKey(), out);
                BytesUtil.writeVInt(x.getValue(), out);
            }
        }

        @Override
        public TrimmedCubeCodeSystem deserialize(ByteBuffer in) {
            Map<Integer, Integer> dependentMetricsMap = Maps.newHashMap();
            Map<Integer, Integer> dictSizes = Maps.newHashMap();
            Map<Integer, Integer> fixedLengthSize = Maps.newHashMap();

            int size = 0;

            size = BytesUtil.readVInt(in);
            for (int i = 0; i < size; ++i) {
                int key = BytesUtil.readVInt(in);
                int value = BytesUtil.readVInt(in);
                dependentMetricsMap.put(key, value);
            }

            size = BytesUtil.readVInt(in);
            for (int i = 0; i < size; ++i) {
                int key = BytesUtil.readVInt(in);
                int value = BytesUtil.readVInt(in);
                dictSizes.put(key, value);
            }

            size = BytesUtil.readVInt(in);
            for (int i = 0; i < size; ++i) {
                int key = BytesUtil.readVInt(in);
                int value = BytesUtil.readVInt(in);
                fixedLengthSize.put(key, value);
            }
            return new TrimmedCubeCodeSystem(dependentMetricsMap, dictSizes, fixedLengthSize);
        }
    };

}

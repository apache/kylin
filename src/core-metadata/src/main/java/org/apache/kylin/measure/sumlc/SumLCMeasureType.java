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

package org.apache.kylin.measure.sumlc;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;

public class SumLCMeasureType extends MeasureType<SumLCCounter> {
    public static final String FUNC_SUM_LC = "SUM_LC";
    public static final String DATATYPE_SUM_LC = "sum_lc";
    static final Map<String, Class<?>> UDAF_MAP = ImmutableMap.of(SumLCMeasureType.FUNC_SUM_LC, SumLCAggFunc.class);

    public SumLCMeasureType(String funcName, DataType dataType) {

    }

    @Override
    public MeasureIngester<SumLCCounter> newIngester() {
        return new MeasureIngester<SumLCCounter>() {
            @Override
            public SumLCCounter valueOf(String[] values, MeasureDesc measureDesc,
                    Map<TblColRef, Dictionary<String>> dictionaryMap) {
                return null;
            }
        };
    }

    @Override
    public MeasureAggregator<SumLCCounter> newAggregator() {
        return new MeasureAggregator<SumLCCounter>() {
            @Override
            public void reset() {
                // left over issue, default implementation ignored
            }

            @Override
            public void aggregate(SumLCCounter value) {
                // left over issue, default implementation ignored
            }

            @Override
            public SumLCCounter aggregate(SumLCCounter value1, SumLCCounter value2) {
                return null;
            }

            @Override
            public SumLCCounter getState() {
                return null;
            }

            @Override
            public int getMemBytesEstimate() {
                return 0;
            }
        };
    }

    @Override
    public boolean needRewrite() {
        return true;
    }

    @Override
    public Map<String, Class<?>> getRewriteCalciteAggrFunctions() {
        return UDAF_MAP;
    }

    public static class Factory extends MeasureTypeFactory<SumLCCounter> {

        @Override
        public MeasureType<SumLCCounter> createMeasureType(String funcName, DataType dataType) {
            return new SumLCMeasureType(funcName, dataType);
        }

        @Override
        public String getAggrFunctionName() {
            return FUNC_SUM_LC;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_SUM_LC;
        }

        @Override
        public Class<? extends DataTypeSerializer<SumLCCounter>> getAggrDataTypeSerializer() {
            return SumLCSerializer.class;
        }
    }

    /**
     * This class is used for registering sum_lc to calcite schema, no need to implement the functions
     */
    public static class SumLCAggFunc {

        public static SumLCCounter init() {
            return null;
        }

        public static SumLCCounter add(SumLCCounter cur, Object v, Object r) {
            return null;
        }

        public static SumLCCounter merge(SumLCCounter counter0, SumLCCounter counter1) {
            return null;
        }

        public static Object result(SumLCCounter counter) {
            return null;
        }
    }

    public static class SumLCSerializer extends DataTypeSerializer<SumLCCounter> {

        public SumLCSerializer(DataType dataType) {

        }

        @Override
        public void serialize(SumLCCounter value, ByteBuffer out) {
            // left over issue, default implementation ignored
        }

        @Override
        public SumLCCounter deserialize(ByteBuffer in) {
            return null;
        }

        @Override
        public int peekLength(ByteBuffer in) {
            return 0;
        }

        @Override
        public int maxLength() {
            return 0;
        }

        @Override
        public int getStorageBytesEstimate() {
            return 0;
        }
    }
}

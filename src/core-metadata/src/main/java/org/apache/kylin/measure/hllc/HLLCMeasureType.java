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

package org.apache.kylin.measure.hllc;

import java.util.Map;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;

public class HLLCMeasureType extends MeasureType<HLLCounter> {
    private static final long serialVersionUID = 1L;

    public static final String FUNC_COUNT_DISTINCT = FunctionDesc.FUNC_COUNT_DISTINCT;
    public static final String DATATYPE_HLLC = "hllc";

    public static class Factory extends MeasureTypeFactory<HLLCounter> {

        @Override
        public MeasureType<HLLCounter> createMeasureType(String funcName, DataType dataType) {
            return new HLLCMeasureType(funcName, dataType);
        }

        @Override
        public String getAggrFunctionName() {
            return FUNC_COUNT_DISTINCT;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_HLLC;
        }

        @Override
        public Class<? extends DataTypeSerializer<HLLCounter>> getAggrDataTypeSerializer() {
            return HLLCSerializer.class;
        }
    }

    // ============================================================================

    private final DataType dataType;

    public HLLCMeasureType(String funcName, DataType dataType) {
        // note at query parsing phase, the data type may be null, because only function and parameters are known
        this.dataType = dataType;
    }

    public void validate(FunctionDesc functionDesc) throws IllegalArgumentException {
        validate(functionDesc.getExpression(), functionDesc.getReturnDataType(), true);
    }

    private void validate(String funcName, DataType dataType, boolean checkDataType) {
        if (!FUNC_COUNT_DISTINCT.equals(funcName))
            throw new IllegalArgumentException();

        if (!DATATYPE_HLLC.equals(dataType.getName()))
            throw new IllegalArgumentException();

        if (dataType.getPrecision() < 1 || dataType.getPrecision() > 5000)
            throw new IllegalArgumentException();
    }

    @Override
    public boolean isMemoryHungry() {
        return true;
    }

    @Override
    public MeasureIngester<HLLCounter> newIngester() {
        return new MeasureIngester<HLLCounter>() {
            private static final long serialVersionUID = 1L;

            HLLCounter current = new HLLCounter(dataType.getPrecision());

            @Override
            public HLLCounter valueOf(String[] values, MeasureDesc measureDesc,
                    Map<TblColRef, Dictionary<String>> dictionaryMap) {
                HLLCounter hllc = current;
                hllc.clear();
                if (values.length == 1) {
                    if (values[0] != null)
                        hllc.add(values[0]);
                } else {
                    boolean allNull = true;
                    StringBuilder buf = new StringBuilder();
                    for (String v : values) {
                        allNull = (allNull && v == null);
                        buf.append(v);
                    }
                    if (!allNull)
                        hllc.add(buf.toString());
                }
                return hllc;
            }

            @Override
            public void reset() {
                current = new HLLCounter(dataType.getPrecision());
            }
        };
    }

    @Override
    public MeasureAggregator<HLLCounter> newAggregator() {
        return new HLLCAggregator(dataType.getPrecision());
    }

    @Override
    public boolean needRewrite() {
        return true;
    }

    static final Map<String, Class<?>> UDAF_MAP = ImmutableMap.<String, Class<?>> of(FUNC_COUNT_DISTINCT,
            HLLDistinctCountAggFunc.class);

    @Override
    public Map<String, Class<?>> getRewriteCalciteAggrFunctions() {
        return UDAF_MAP;
    }

    public static boolean isCountDistinct(FunctionDesc func) {
        return FUNC_COUNT_DISTINCT.equalsIgnoreCase(func.getExpression());
    }

}

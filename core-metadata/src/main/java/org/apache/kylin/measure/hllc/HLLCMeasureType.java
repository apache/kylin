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

import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
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

public class HLLCMeasureType extends MeasureType<HyperLogLogPlusCounter> {

    public static final String FUNC_COUNT_DISTINCT = "COUNT_DISTINCT";
    public static final String DATATYPE_HLLC = "hllc";

    public static class Factory extends MeasureTypeFactory<HyperLogLogPlusCounter> {

        @Override
        public MeasureType<HyperLogLogPlusCounter> createMeasureType(String funcName, DataType dataType) {
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
        public Class<? extends DataTypeSerializer<HyperLogLogPlusCounter>> getAggrDataTypeSerializer() {
            return HLLCSerializer.class;
        }
    }

    // ============================================================================

    private final DataType dataType;

    public HLLCMeasureType(String funcName, DataType dataType) {
        validate(funcName, dataType);
        this.dataType = dataType;
    }

    public void validate(FunctionDesc functionDesc) throws IllegalArgumentException {
        validate(functionDesc.getExpression(), functionDesc.getReturnDataType());
    }

    private void validate(String funcName, DataType dataType) {
        if (FUNC_COUNT_DISTINCT.equals(funcName) == false)
            throw new IllegalArgumentException();

        // data type could be null at query layer, because only function and parameters are known at that stage
        if (dataType != null) {
            if (DATATYPE_HLLC.equals(dataType.getName()) == false)
                throw new IllegalArgumentException();

            if (dataType.getPrecision() < 1 || dataType.getPrecision() > 5000)
                throw new IllegalArgumentException();
        }
    }

    @Override
    public boolean isMemoryHungry() {
        return true;
    }

    @Override
    public MeasureIngester<HyperLogLogPlusCounter> newIngester() {
        return new MeasureIngester<HyperLogLogPlusCounter>() {
            HyperLogLogPlusCounter current = new HyperLogLogPlusCounter(dataType.getPrecision());

            @Override
            public HyperLogLogPlusCounter valueOf(String[] values, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
                HyperLogLogPlusCounter hllc = current;
                hllc.clear();
                for (String v : values)
                    hllc.add(v == null ? "__nUlL__" : v);
                return hllc;
            }
        };
    }

    @Override
    public MeasureAggregator<HyperLogLogPlusCounter> newAggregator() {
        return new HLLCAggregator(dataType.getPrecision());
    }

    @Override
    public boolean needRewrite() {
        return true;
    }

    @Override
    public Class<?> getRewriteCalciteAggrFunctionClass() {
        // TODO Auto-generated method stub
        return null;
    }

    public static boolean isCountDistinct(FunctionDesc func) {
        return FUNC_COUNT_DISTINCT.equalsIgnoreCase(func.getExpression());
    }

}

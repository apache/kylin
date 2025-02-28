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

package org.apache.kylin.measure.dim;

import java.util.Map;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;

public class DimCountDistinctMeasureType extends MeasureType<Object> {
    public static final String DATATYPE_DIM_DC = "dim_dc";

    public static class Factory extends MeasureTypeFactory<Object> {

        @Override
        public MeasureType<Object> createMeasureType(String funcName, DataType dataType) {
            return new DimCountDistinctMeasureType();
        }

        @Override
        public String getAggrFunctionName() {
            return FunctionDesc.FUNC_COUNT_DISTINCT;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_DIM_DC;
        }

        @Override
        public Class getAggrDataTypeSerializer() {
            return DimCountDistincSerializer.class;
        }

    }

    @Override
    public MeasureIngester newIngester() {
        throw new UnsupportedOperationException("No ingester for this measure type.");
    }

    @Override
    public MeasureAggregator newAggregator() {
        return new DimCountDistinctAggregator();
    }

    @Override
    public boolean needRewrite() {
        return true;
    }

    @Override
    public boolean needRewriteField() {
        return false;
    }

    static final Map<String, Class<?>> UDAF_MAP = ImmutableMap.of(FunctionDesc.FUNC_COUNT_DISTINCT,
            DimCountDistinctAggFunc.class);

    @Override
    public Map<String, Class<?>> getRewriteCalciteAggrFunctions() {
        return UDAF_MAP;
    }
}

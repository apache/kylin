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

package org.apache.kylin.measure.percentile;

import java.util.Map;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

public class PercentileMeasureType extends MeasureType<PercentileCounter> {
    // compression ratio saved in DataType.precision
    private final DataType dataType;
    public static final String FUNC_PERCENTILE = "PERCENTILE";
    public static final String FUNC_PERCENTILE_APPROX = "PERCENTILE_APPROX";
    public static final String DATATYPE_PERCENTILE = "percentile";

    public PercentileMeasureType(String funcName, DataType dataType) {
        this.dataType = dataType;
    }

    public static class Factory extends MeasureTypeFactory<PercentileCounter> {

        @Override
        public MeasureType<PercentileCounter> createMeasureType(String funcName, DataType dataType) {
            return new PercentileMeasureType(funcName, dataType);
        }

        @Override
        public String getAggrFunctionName() {
            return FUNC_PERCENTILE_APPROX;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_PERCENTILE;
        }

        @Override
        public Class<? extends DataTypeSerializer<PercentileCounter>> getAggrDataTypeSerializer() {
            return PercentileSerializer.class;
        }
    }

    @Override
    public MeasureIngester<PercentileCounter> newIngester() {
        return new MeasureIngester<PercentileCounter>() {
            PercentileCounter current = new PercentileCounter(dataType.getPrecision());

            @Override
            public PercentileCounter valueOf(String[] values, MeasureDesc measureDesc,
                    Map<TblColRef, Dictionary<String>> dictionaryMap) {
                PercentileCounter counter = current;
                counter.clear();
                for (String v : values) {
                    if (v != null)
                        counter.add(Double.parseDouble(v));
                }
                return counter;
            }

            @Override
            public void reset() {
                current = new PercentileCounter(dataType.getPrecision());
            }
        };
    }

    @Override
    public MeasureAggregator<PercentileCounter> newAggregator() {
        return new PercentileAggregator(dataType.getPrecision());
    }

    @Override
    public boolean needRewrite() {
        return true;
    }

    static final Map<String, Class<?>> UDAF_MAP = ImmutableMap.<String, Class<?>> of(
            PercentileMeasureType.FUNC_PERCENTILE, PercentileAggFunc.class,
            PercentileMeasureType.FUNC_PERCENTILE_APPROX, PercentileAggFunc.class);

    @Override
    public Map<String, Class<?>> getRewriteCalciteAggrFunctions() {
        return UDAF_MAP;
    }
}

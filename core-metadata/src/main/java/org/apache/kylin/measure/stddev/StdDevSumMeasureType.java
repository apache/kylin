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

package org.apache.kylin.measure.stddev;

import java.util.List;
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
import org.apache.kylin.metadata.realization.SQLDigest;

import com.google.common.collect.ImmutableMap;

public class StdDevSumMeasureType extends MeasureType<StdDevCounter> {

    private final DataType dataType;
    public static final String FUNC_STDDEV_SUM = "STDDEV_SUM";
    public static final String DATATYPE_STDDEV = "stddev_sum";

    public StdDevSumMeasureType(String funcName, DataType dataType) {
        this.dataType = dataType;
    }

    public static class Factory extends MeasureTypeFactory<StdDevCounter> {

        @Override
        public MeasureType<StdDevCounter> createMeasureType(String funcName, DataType dataType) {
            return new StdDevSumMeasureType(funcName, dataType);
        }

        @Override
        public String getAggrFunctionName() {
            return FUNC_STDDEV_SUM;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_STDDEV;
        }

        @Override
        public Class<? extends DataTypeSerializer<StdDevCounter>> getAggrDataTypeSerializer() {
            return StdDevSerializer.class;
        }
    }

    @Override
    public MeasureIngester<StdDevCounter> newIngester() {
        return new MeasureIngester<StdDevCounter>() {
            StdDevCounter current = new StdDevCounter();

            @Override
            public StdDevCounter valueOf(String[] values, MeasureDesc measureDesc,
                    Map<TblColRef, Dictionary<String>> dictionaryMap) {
                StdDevCounter counter = current;
                for (String v : values) {
                    if (v != null)
                        counter.add(Double.parseDouble(v));
                }
                return counter;
            }

            @Override
            public void reset() {
                current = new StdDevCounter();
            }
        };
    }

    @Override
    public MeasureAggregator<StdDevCounter> newAggregator() {
        return new StdDevTransformation();
    }

    @Override
    public boolean needRewrite() {
        return true;
    }

    static final Map<String, Class<?>> UDAF_MAP = ImmutableMap
            .<String, Class<?>> of(StdDevSumMeasureType.FUNC_STDDEV_SUM, StandardDeviationAggFunc.class);

    @Override
    public Map<String, Class<?>> getRewriteCalciteAggrFunctions() {
        return UDAF_MAP;
    }

    @Override
    public void adjustSqlDigest(List<MeasureDesc> measureDescs, SQLDigest sqlDigest) {
    }
}
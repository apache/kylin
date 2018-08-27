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

package org.apache.kylin.measure.auc;


import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;

import com.google.common.collect.ImmutableMap;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

import java.util.Map;

public class AucMesureType extends MeasureType<AucCounter> {

    private final DataType dataType;
    public static final String FUNC_AUC = "AUC";
    public static final String DATATYPE_AUC = "auc";

    public AucMesureType(String funcName, DataType dataType) {
        this.dataType = dataType;
    }

    public static class Factory extends MeasureTypeFactory<AucCounter> {

        @Override
        public MeasureType<AucCounter> createMeasureType(String funcName, DataType dataType) {
            return new AucMesureType(funcName, dataType);
        }

        @Override
        public String getAggrFunctionName() {
            return FUNC_AUC;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_AUC;
        }

        @Override
        public Class<? extends DataTypeSerializer<AucCounter>> getAggrDataTypeSerializer() {
            return AucSerializer.class;
        }
    }

    @Override
    public MeasureIngester<AucCounter> newIngester() {
        throw new UnsupportedOperationException("No ingester for this measure type.");
    }

    @Override
    public MeasureAggregator<AucCounter> newAggregator() {
        return new AucAggregator();
    }

    static final Map<String, Class<?>> UDAF_MAP = ImmutableMap.<String, Class<?>>of(
            AucMesureType.FUNC_AUC, AucAggFunc.class);

    @Override
    public Map<String, Class<?>> getRewriteCalciteAggrFunctions() {
        return UDAF_MAP;
    }

    @Override
    public boolean needRewrite() {
        return true;
    }
}

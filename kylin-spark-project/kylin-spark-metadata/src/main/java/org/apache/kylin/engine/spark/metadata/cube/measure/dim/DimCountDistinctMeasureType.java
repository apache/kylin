/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

 
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

package org.apache.kylin.engine.spark.metadata.cube.measure.dim;

import com.google.common.collect.ImmutableMap;
import org.apache.kylin.engine.spark.metadata.cube.datatype.DataType;
import org.apache.kylin.engine.spark.metadata.cube.measure.MeasureAggregator;
import org.apache.kylin.engine.spark.metadata.cube.measure.MeasureType;
import org.apache.kylin.engine.spark.metadata.cube.measure.MeasureTypeFactory;
import org.apache.kylin.engine.spark.metadata.cube.model.FunctionDesc;
import org.apache.kylin.engine.spark.metadata.cube.model.MeasureDesc;
import org.apache.kylin.engine.spark.metadata.cube.realization.SQLDigest;
import org.apache.kylin.measure.MeasureIngester;

import java.util.List;
import java.util.Map;

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

    static final Map<String, Class<?>> UDAF_MAP = ImmutableMap.<String, Class<?>> of(FunctionDesc.FUNC_COUNT_DISTINCT,
            DimCountDistinctAggFunc.class);

    @Override
    public Map<String, Class<?>> getRewriteCalciteAggrFunctions() {
        return UDAF_MAP;
    }

    public void adjustSqlDigest(List<MeasureDesc> measureDescs, SQLDigest sqlDigest) {
        for (MeasureDesc measureDesc : measureDescs) {
            sqlDigest.groupbyColumns.addAll(measureDesc.getFunction().getColRefs());
            sqlDigest.aggregations.remove(measureDesc.getFunction());
        }
    }
}

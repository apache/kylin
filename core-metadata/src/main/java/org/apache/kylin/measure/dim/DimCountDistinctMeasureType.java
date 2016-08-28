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

import java.util.List;

import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.realization.SQLDigest;

/**
 * Created by dongli on 4/20/16.
 */
public class DimCountDistinctMeasureType extends MeasureType<Object> {
    public static class DimCountDistinctMeasureTypeFactory extends MeasureTypeFactory<Object> {

        @Override
        public MeasureType<Object> createMeasureType(String funcName, DataType dataType) {
            return new DimCountDistinctMeasureType();
        }

        @Override
        public String getAggrFunctionName() {
            return null;
        }

        @Override
        public String getAggrDataTypeName() {
            return null;
        }

        @Override
        public Class getAggrDataTypeSerializer() {
            return null;
        }

    }

    @Override
    public MeasureIngester newIngester() {
        throw new UnsupportedOperationException("No ingester for this measure type.");
    }

    @Override
    public MeasureAggregator newAggregator() {
        throw new UnsupportedOperationException("No aggregator for this measure type.");
    }

    @Override
    public boolean needRewrite() {
        return true;
    }

    @Override
    public boolean needRewriteField() {
        return false;
    }

    @Override
    public Class<?> getRewriteCalciteAggrFunctionClass() {
        return DimCountDistinctAggFunc.class;
    }

    public void adjustSqlDigest(List<MeasureDesc> measureDescs, SQLDigest sqlDigest) {
        for (MeasureDesc measureDesc : measureDescs) {
            sqlDigest.groupbyColumns.addAll(measureDesc.getFunction().getParameter().getColRefs());
            sqlDigest.aggregations.remove(measureDesc.getFunction());
        }
    }
}

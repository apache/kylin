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

package org.apache.kylin.measure.bitmap.intersect;

import java.util.Map;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.measure.bitmap.BitmapIntersectDistinctCountAggFunc;
import org.apache.kylin.measure.bitmap.BitmapIntersectDistinctCountAggV2Func;
import org.apache.kylin.measure.bitmap.BitmapUuidFunc;
import org.apache.kylin.measure.bitmap.BitmapUuidValueFunc;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;

public class IntersectMeasureType extends MeasureType<IntersectBitmapCounter> {
    public static final String FUNC_INTERSECT_COUNT_DISTINCT = FunctionDesc.FUNC_INTERSECT_COUNT;
    public static final String DATATYPE_BITMAP = "intersect_bitmap";

    public static class Factory extends MeasureTypeFactory<IntersectBitmapCounter> {

        @Override
        public MeasureType<IntersectBitmapCounter> createMeasureType(String funcName, DataType dataType) {
            return new IntersectMeasureType();
        }

        // cube chose
        // agg modify
        @Override
        public String getAggrFunctionName() {
            return FUNC_INTERSECT_COUNT_DISTINCT;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_BITMAP;
        }

        @Override
        public Class<? extends DataTypeSerializer<IntersectBitmapCounter>> getAggrDataTypeSerializer() {
            return IntersectSerializer.class;
        }
    }

    @Override
    public MeasureIngester<IntersectBitmapCounter> newIngester() {
        return null;
    }

    @Override
    public MeasureAggregator<IntersectBitmapCounter> newAggregator() {
        return new IntersectMeasureAggregator();
    }

    private static final Map<String, Class<?>> UDAF_MAP = ImmutableMap.<String, Class<?>> builder()
            .put(FunctionDesc.FUNC_INTERSECT_COUNT, BitmapIntersectDistinctCountAggFunc.class)
            .put(FunctionDesc.FUNC_INTERSECT_COUNT_V2, BitmapIntersectDistinctCountAggV2Func.class)
            .put(FunctionDesc.FUNC_INTERSECT_VALUE, BitmapIntersectDistinctCountAggFunc.class)
            .put(FunctionDesc.FUNC_INTERSECT_VALUE_V2, BitmapIntersectDistinctCountAggV2Func.class)
            .put(FunctionDesc.FUNC_INTERSECT_BITMAP_UUID, BitmapIntersectDistinctCountAggFunc.class)
            .put(FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_V2, BitmapIntersectDistinctCountAggV2Func.class)
            .put(FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_DISTINCT, BitmapUuidFunc.class)
            .put(FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_COUNT, BitmapUuidFunc.class)
            .put(FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_VALUE, BitmapUuidValueFunc.class)
            .put(FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_VALUE_ALL, BitmapUuidFunc.class)
            .put(FunctionDesc.FUNC_UNION_BITMAP_UUID_DISTINCT, BitmapUuidFunc.class)
            .put(FunctionDesc.FUNC_UNION_BITMAP_UUID_COUNT, BitmapUuidFunc.class)
            .put(FunctionDesc.FUNC_UNION_BITMAP_UUID_VALUE, BitmapUuidValueFunc.class)
            .put(FunctionDesc.FUNC_UNION_BITMAP_UUID_VALUE_ALL, BitmapUuidFunc.class)
            .build();

    @Override
    public Map<String, Class<?>> getRewriteCalciteAggrFunctions() {
        return UDAF_MAP;
    }

    @Override
    public boolean needRewrite() {
        return true;
    }
}

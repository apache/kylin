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

package org.apache.kylin.measure.map.bitmap;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

import com.google.common.collect.ImmutableMap;

public class BitmapMapMeasureType extends MeasureType<BitmapCounterMap> {
    public static final String FUNC_COUNT_DISTINCT = FunctionDesc.FUNC_COUNT_DISTINCT;
    public static final String DATATYPE_BITMAP_MAP = "bitmap_map";

    public static class Factory extends MeasureTypeFactory<BitmapCounterMap> {

        @Override
        public MeasureType<BitmapCounterMap> createMeasureType(String funcName, DataType dataType) {
            return new BitmapMapMeasureType(funcName, dataType);
        }

        @Override
        public String getAggrFunctionName() {
            return FUNC_COUNT_DISTINCT;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_BITMAP_MAP;
        }

        @Override
        public Class<? extends DataTypeSerializer<BitmapCounterMap>> getAggrDataTypeSerializer() {
            return BitmapMapSerializer.class;
        }
    }

    public DataType dataType;

    public BitmapMapMeasureType(String funcName, DataType dataType) {
        this.dataType = dataType;
    }

    @Override
    public void validate(FunctionDesc functionDesc) throws IllegalArgumentException {
        checkArgument(FUNC_COUNT_DISTINCT.equals(functionDesc.getExpression()),
                "BitmapMapMeasureType only support function %s, got %s", FUNC_COUNT_DISTINCT,
                functionDesc.getExpression());
        checkArgument(functionDesc.getParameterCount() == 1, "BitmapMapMeasureType only support 1 parameter, got %d",
                functionDesc.getParameterCount());

        String returnType = functionDesc.getReturnDataType().getName();
        checkArgument(DATATYPE_BITMAP_MAP.equals(returnType), "BitmapMapMeasureType's return type must be %s, got %s",
                DATATYPE_BITMAP_MAP, returnType);
    }

    @Override
    public boolean isMemoryHungry() {
        return true;
    }

    @Override
    public MeasureIngester<BitmapCounterMap> newIngester() {
        final BitmapCounterMapFactory factory = RoaringBitmapCounterMapFactory.INSTANCE;

        return new MeasureIngester<BitmapCounterMap>() {
            BitmapCounterMap current = factory.newBitmapMap();

            @Override
            public BitmapCounterMap valueOf(String[] values, MeasureDesc measureDesc,
                    Map<TblColRef, Dictionary<String>> dictionaryMap) {
                checkArgument(values.length == 2, "expect 2 values, got %s", Arrays.toString(values));

                current.clear();

                if (values[0] == null) {
                    return current;
                }

                int id;
                if (needDictionaryColumn(measureDesc.getFunction())) {
                    TblColRef literalCol = measureDesc.getFunction().getParameter().getColRefs().get(0);
                    Dictionary<String> dictionary = dictionaryMap.get(literalCol);
                    id = dictionary.getIdFromValue(values[0]);
                } else {
                    id = Integer.parseInt(values[0]);
                }

                Object key = factory.getMapKeySerializer().parseKey(values[1]);
                current.add(key, id);
                return current;
            }

            @Override
            public BitmapCounterMap reEncodeDictionary(BitmapCounterMap value, MeasureDesc measureDesc,
                    Map<TblColRef, Dictionary<String>> oldDicts, Map<TblColRef, Dictionary<String>> newDicts) {
                return value;
            }

            @Override
            public void reset() {
                current = factory.newBitmapMap();
            }
        };
    }

    @Override
    public MeasureAggregator<BitmapCounterMap> newAggregator() {
        return new BitmapMapAggregator();
    }

    @Override
    public List<TblColRef> getColumnsNeedDictionary(FunctionDesc functionDesc) {
        if (needDictionaryColumn(functionDesc)) {
            return Collections.singletonList(functionDesc.getParameter().getColRefs().get(0));
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public List<TblColRef> getColumnsNeedDictionaryForBuildingOnly(FunctionDesc functionDesc) {
        return getColumnsNeedDictionary(functionDesc);
    }

    // In order to keep compatibility with old version, tinyint/smallint/int column use value directly, without dictionary
    private boolean needDictionaryColumn(FunctionDesc functionDesc) {
        DataType dataType = functionDesc.getParameter().getColRefs().get(0).getType();
        if (functionDesc.isMrDict()) {
            return false;
        }
        if (dataType.isIntegerFamily() && !dataType.isBigInt()) {
            return false;
        }
        return true;
    }

    @Override
    public boolean needRewrite() {
        return true;
    }

    static final Map<String, Class<?>> UDAF_MAP = ImmutableMap.<String, Class<?>> of(FUNC_COUNT_DISTINCT,
            BitmapMapDistinctCountAggFunc.class);

    @Override
    public Map<String, Class<?>> getRewriteCalciteAggrFunctions() {
        return UDAF_MAP;
    }

}

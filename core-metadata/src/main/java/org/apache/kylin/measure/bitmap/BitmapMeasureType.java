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

package org.apache.kylin.measure.bitmap;

import static org.apache.kylin.shaded.com.google.common.base.Preconditions.checkArgument;

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
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.realization.SQLDigest.SQLCall;

import org.apache.kylin.shaded.com.google.common.collect.ImmutableMap;

/**
 * Created by sunyerui on 15/12/10.
 */
public class BitmapMeasureType extends MeasureType<BitmapCounter> {
    public static final String FUNC_COUNT_DISTINCT = FunctionDesc.FUNC_COUNT_DISTINCT;
    public static final String FUNC_INTERSECT_COUNT_DISTINCT = "INTERSECT_COUNT";
    public static final String FUNC_INTERSECT_VALUE = "INTERSECT_VALUE";
    public static final String DATATYPE_BITMAP = "bitmap";

    public static class Factory extends MeasureTypeFactory<BitmapCounter> {

        @Override
        public MeasureType<BitmapCounter> createMeasureType(String funcName, DataType dataType) {
            return new BitmapMeasureType();
        }

        @Override
        public String getAggrFunctionName() {
            return FUNC_COUNT_DISTINCT;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_BITMAP;
        }

        @Override
        public Class<? extends DataTypeSerializer<BitmapCounter>> getAggrDataTypeSerializer() {
            return BitmapSerializer.class;
        }
    }

    public BitmapMeasureType() {
    }

    @Override
    public void validate(FunctionDesc functionDesc) throws IllegalArgumentException {
        checkArgument(FUNC_COUNT_DISTINCT.equals(functionDesc.getExpression()),
                "BitmapMeasureType only support function %s, got %s", FUNC_COUNT_DISTINCT, functionDesc.getExpression());
        checkArgument(functionDesc.getParameterCount() == 1,
                "BitmapMeasureType only support 1 parameter, got %d", functionDesc.getParameterCount());

        String returnType = functionDesc.getReturnDataType().getName();
        checkArgument(DATATYPE_BITMAP.equals(returnType),
                "BitmapMeasureType's return type must be %s, got %s", DATATYPE_BITMAP, returnType);
    }

    @Override
    public boolean isMemoryHungry() {
        return true;
    }

    @Override
    public MeasureIngester<BitmapCounter> newIngester() {
        final BitmapCounterFactory factory = RoaringBitmapCounterFactory.INSTANCE;

        return new MeasureIngester<BitmapCounter>() {
            BitmapCounter current = factory.newBitmap();

            @Override
            public BitmapCounter valueOf(String[] values, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
                checkArgument(values.length == 1, "expect 1 value, got %s", Arrays.toString(values));

                current.clear();

                if (values[0] == null) {
                    return current;
                }

                int id;
                TblColRef literalCol = measureDesc.getFunction().getParameter().getColRefs().get(0);
                if (needDictionaryColumn(measureDesc.getFunction()) && dictionaryMap.containsKey(literalCol)) {
                        Dictionary<String> dictionary = dictionaryMap.get(literalCol);
                        id = dictionary.getIdFromValue(values[0]);
                } else {
                    id = Integer.parseInt(values[0]);
                }

                current.add(id);
                return current;
            }

            @Override
            public BitmapCounter reEncodeDictionary(BitmapCounter value, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> oldDicts, Map<TblColRef, Dictionary<String>> newDicts) {
                //BitmapCounter needn't reEncode
                return value;
            }

            @Override
            public void reset() {
                current = factory.newBitmap();
            }
        };
    }

    @Override
    public MeasureAggregator<BitmapCounter> newAggregator() {
        return new BitmapAggregator();
    }

    @Override
    public List<TblColRef> getColumnsNeedDictionary(FunctionDesc functionDesc) {
        if (needDictionaryColumn(functionDesc)) {
            return Collections.singletonList(functionDesc.getParameter().getColRefs().get(0));
        } else {
            return Collections.emptyList();
        }
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

    static final Map<String, Class<?>> UDAF_MAP = ImmutableMap.of(
            FUNC_COUNT_DISTINCT, BitmapDistinctCountAggFunc.class,
            FUNC_INTERSECT_COUNT_DISTINCT, BitmapIntersectDistinctCountAggFunc.class,
            FUNC_INTERSECT_VALUE, BitmapIntersectValueAggFunc.class);

    @Override
    public Map<String, Class<?>> getRewriteCalciteAggrFunctions() {
        return UDAF_MAP;
    }

    @Override
    public void adjustSqlDigest(List<MeasureDesc> measureDescs, SQLDigest sqlDigest) {
        for (SQLCall call : sqlDigest.aggrSqlCalls) {
            if (FUNC_INTERSECT_COUNT_DISTINCT.equals(call.function)
                    || FUNC_INTERSECT_VALUE.equals(call.function)) {
                TblColRef col = (TblColRef) call.args.get(1);
                if (!sqlDigest.groupbyColumns.contains(col))
                    sqlDigest.groupbyColumns.add(col);
            }
        }
    }
}

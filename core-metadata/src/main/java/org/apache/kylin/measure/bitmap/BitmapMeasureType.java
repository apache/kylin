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

import org.apache.kylin.dimension.Dictionary;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by sunyerui on 15/12/10.
 */
public class BitmapMeasureType extends MeasureType<BitmapCounter> {
    public static final String FUNC_COUNT_DISTINCT = "COUNT_DISTINCT";
    public static final String DATATYPE_BITMAP = "bitmap";

    public static class Factory extends MeasureTypeFactory<BitmapCounter> {

        @Override
        public MeasureType<BitmapCounter> createMeasureType(String funcName, DataType dataType) {
            return new BitmapMeasureType(funcName, dataType);
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

    public DataType dataType;

    public BitmapMeasureType(String funcName, DataType dataType) {
        this.dataType = dataType;
    }

    @Override
    public void validate(FunctionDesc functionDesc) throws IllegalArgumentException {
        if (FUNC_COUNT_DISTINCT.equals(functionDesc.getExpression()) == false)
            throw new IllegalArgumentException("BitmapMeasureType func is not " + FUNC_COUNT_DISTINCT + " but " + functionDesc.getExpression());

        if (DATATYPE_BITMAP.equals(functionDesc.getReturnDataType().getName()) == false)
            throw new IllegalArgumentException("BitmapMeasureType datatype is not " + DATATYPE_BITMAP + " but " + functionDesc.getReturnDataType().getName());

        List<TblColRef> colRefs = functionDesc.getParameter().getColRefs();
        if (colRefs.size() != 1 && colRefs.size() != 2) {
            throw new IllegalArgumentException("Bitmap measure need 1 or 2 parameters, but has " + colRefs.size());
        }
    }

    @Override
    public boolean isMemoryHungry() {
        return true;
    }

    @Override
    public MeasureIngester<BitmapCounter> newIngester() {
        return new MeasureIngester<BitmapCounter>() {
            BitmapCounter current = new BitmapCounter();

            @Override
            public BitmapCounter valueOf(String[] values, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
                List<TblColRef> literalCols = measureDesc.getFunction().getParameter().getColRefs();
                TblColRef literalCol = null;
                if (literalCols.size() == 1) {
                    literalCol = literalCols.get(0);
                } else if (literalCols.size() == 2) {
                    literalCol = literalCols.get(1);
                } else {
                    throw new IllegalArgumentException("Bitmap measure need 1 or 2 parameters");
                }
                Dictionary<String> dictionary = dictionaryMap.get(literalCol);
                BitmapCounter bitmap = current;
                bitmap.clear();
                // bitmap measure may have two values due to two parameters, only the first value should be ingested
                if (values != null && values.length > 0 && values[0] != null) {
                    int id = dictionary.getIdFromValue(values[0]);
                    bitmap.add(id);
                }
                return bitmap;
            }
        };
    }

    @Override
    public MeasureAggregator<BitmapCounter> newAggregator() {
        return new BitmapAggregator();
    }

    /**
     * generate dict with first col by default, and with second col if specified
     *
     * Typical case: we have col uuid, and another col flag_uuid (if flag==1, uuid, null),
     * the metrics count(distinct uuid) and count(distinct flag_uuid) should both generate dict with uuid, instead of uuid and flag_uuid
     */
    @Override
    public List<TblColRef> getColumnsNeedDictionary(FunctionDesc functionDesc) {
        List<TblColRef> literalCols = functionDesc.getParameter().getColRefs();
        if (literalCols.size() == 1) {
            return Collections.singletonList(literalCols.get(0));
        } else if (literalCols.size() == 2) {
            return Collections.singletonList(literalCols.get(1));
        } else {
            throw new IllegalArgumentException("Bitmap measure need 1 or 2 parameters");
        }
    }

    @Override
    public boolean needRewrite() {
        return true;
    }

    @Override
    public boolean needCubeLevelDictionary() {
        return true;
    }

    @Override
    public Class<?> getRewriteCalciteAggrFunctionClass() {
        return BitmapDistinctCountAggFunc.class;
    }

}

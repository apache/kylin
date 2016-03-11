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
        if (colRefs.size() != 1) {
            throw new IllegalArgumentException("BitmapMeasureType col parameters count is not 1 but " + colRefs.size());
        }

        TblColRef colRef = colRefs.get(0);
        DataType type = colRef.getType();
        if (!type.isIntegerFamily()) {
            throw new IllegalArgumentException("BitmapMeasureType col type is not IntegerFamily but " + type.getName() + " of column " + colRef.getCanonicalName());
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
                BitmapCounter bitmap = current;
                bitmap.clear();
                for (String v : values)
                    bitmap.add(v);
                return bitmap;
            }
        };
    }

    @Override
    public MeasureAggregator<BitmapCounter> newAggregator() {
        return new BitmapAggregator();
    }

    @Override
    public boolean needRewrite() {
        return true;
    }

    @Override
    public Class<?> getRewriteCalciteAggrFunctionClass() {
        return BitmapDistinctCountAggFunc.class;
    }

}

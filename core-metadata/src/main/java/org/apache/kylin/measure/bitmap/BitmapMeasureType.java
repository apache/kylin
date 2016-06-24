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

import java.util.Collections;
import java.util.Iterator;
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
                if (needDictionaryColumn(measureDesc.getFunction())) {
                    TblColRef literalCol = measureDesc.getFunction().getParameter().getColRefs().get(0);
                    Dictionary<String> dictionary = dictionaryMap.get(literalCol);
                    if (values != null && values.length > 0 && values[0] != null) {
                        int id = dictionary.getIdFromValue(values[0]);
                        bitmap.add(id);
                    }
                } else {
                    for (String value : values) {
                        bitmap.add(value);
                    }
                }
                return bitmap;
            }

            @Override
            public BitmapCounter reEncodeDictionary(BitmapCounter value, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> oldDicts, Map<TblColRef, Dictionary<String>> newDicts) {
                if (!needDictionaryColumn(measureDesc.getFunction())) {
                    return value;
                }
                TblColRef colRef = measureDesc.getFunction().getParameter().getColRefs().get(0);
                Dictionary<String> sourceDict = oldDicts.get(colRef);
                Dictionary<String> mergedDict = newDicts.get(colRef);

                BitmapCounter retValue = new BitmapCounter();
                byte[] literal = new byte[sourceDict.getSizeOfValue()];
                Iterator<Integer> iterator = value.iterator();
                while (iterator.hasNext()) {
                    int id = iterator.next();
                    int newId;
                    int size = sourceDict.getValueBytesFromId(id, literal, 0);
                    if (size < 0) {
                        newId = mergedDict.nullId();
                    } else {
                        newId = mergedDict.getIdFromValueBytes(literal, 0, size);
                    }
                    retValue.add(newId);
                }
                return retValue;
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

    @Override
    public boolean needRewrite() {
        return true;
    }

    @Override
    public Class<?> getRewriteCalciteAggrFunctionClass() {
        return BitmapDistinctCountAggFunc.class;
    }

    // In order to keep compatibility with old version, tinyint/smallint/int column use value directly, without dictionary
    private boolean needDictionaryColumn(FunctionDesc functionDesc) {
        DataType dataType = functionDesc.getParameter().getColRefs().get(0).getType();
        if (dataType.isIntegerFamily() && !dataType.isBigInt()) {
            return false;
        }
        return true;
    }
}

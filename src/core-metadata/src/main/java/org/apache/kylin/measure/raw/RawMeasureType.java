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

package org.apache.kylin.measure.raw;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

public class RawMeasureType extends MeasureType<List<ByteArray>> {
    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(RawMeasureType.class);

    public static final String FUNC_RAW = "RAW";
    public static final String DATATYPE_RAW = "raw";

    public static class Factory extends MeasureTypeFactory<List<ByteArray>> {

        @Override
        public MeasureType<List<ByteArray>> createMeasureType(String funcName, DataType dataType) {
            return new RawMeasureType(funcName, dataType);
        }

        @Override
        public String getAggrFunctionName() {
            return FUNC_RAW;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_RAW;
        }

        @Override
        public Class<? extends DataTypeSerializer<List<ByteArray>>> getAggrDataTypeSerializer() {
            return RawSerializer.class;
        }
    }

    @SuppressWarnings("unused")
    private final DataType dataType;

    public RawMeasureType(String funcName, DataType dataType) {
        this.dataType = dataType;
    }

    public void validate(FunctionDesc functionDesc) throws IllegalArgumentException {
        validate(functionDesc.getExpression(), functionDesc.getReturnDataType(), true);
    }

    private void validate(String funcName, DataType dataType, boolean checkDataType) {
        if (!FUNC_RAW.equals(funcName))
            throw new IllegalArgumentException();

        if (!DATATYPE_RAW.equals(dataType.getName()))
            throw new IllegalArgumentException();

    }

    @Override
    public boolean isMemoryHungry() {
        return true;
    }

    @Override
    public MeasureIngester<List<ByteArray>> newIngester() {
        return new MeasureIngester<List<ByteArray>>() {
            private static final long serialVersionUID = 1L;

            //encode measure value to dictionary
            @Override
            public List<ByteArray> valueOf(String[] values, MeasureDesc measureDesc,
                    Map<TblColRef, Dictionary<String>> dictionaryMap) {
                if (values.length != 1)
                    throw new IllegalArgumentException();

                //source input column value
                String literal = values[0];
                // encode literal using dictionary
                TblColRef literalCol = getRawColumn(measureDesc.getFunction());
                Dictionary<String> dictionary = dictionaryMap.get(literalCol);
                int keyEncodedValue = dictionary.getIdFromValue(literal);

                ByteArray key = new ByteArray(dictionary.getSizeOfId());
                BytesUtil.writeUnsigned(keyEncodedValue, key.array(), key.offset(), dictionary.getSizeOfId());

                List<ByteArray> valueList = new ArrayList<ByteArray>(1);
                valueList.add(key);
                return valueList;
            }

            //merge measure dictionary
            @Override
            public List<ByteArray> reEncodeDictionary(List<ByteArray> value, MeasureDesc measureDesc,
                    Map<TblColRef, Dictionary<String>> oldDicts, Map<TblColRef, Dictionary<String>> newDicts) {
                TblColRef colRef = getRawColumn(measureDesc.getFunction());
                Dictionary<String> sourceDict = oldDicts.get(colRef);
                Dictionary<String> mergedDict = newDicts.get(colRef);

                int valueSize = value.size();
                byte[] newIdBuf = new byte[valueSize * mergedDict.getSizeOfId()];

                int bufOffset = 0;
                for (ByteArray c : value) {
                    int oldId = BytesUtil.readUnsigned(c.array(), c.offset(), c.length());
                    int newId;
                    String v = sourceDict.getValueFromId(oldId);
                    if (v == null) {
                        newId = mergedDict.nullId();
                    } else {
                        newId = mergedDict.getIdFromValue(v);
                    }
                    BytesUtil.writeUnsigned(newId, newIdBuf, bufOffset, mergedDict.getSizeOfId());
                    c.reset(newIdBuf, bufOffset, mergedDict.getSizeOfId());
                    bufOffset += mergedDict.getSizeOfId();
                }
                return value;
            }
        };
    }

    @Override
    public MeasureAggregator<List<ByteArray>> newAggregator() {
        return new RawAggregator();
    }

    @Override
    public List<TblColRef> getColumnsNeedDictionary(FunctionDesc functionDesc) {
        TblColRef literalCol = functionDesc.getColRefs().get(0);
        return Collections.singletonList(literalCol);
    }

    public CapabilityResult.CapabilityInfluence influenceCapabilityCheck(Collection<TblColRef> unmatchedDimensions,
            Collection<FunctionDesc> unmatchedAggregations, SQLDigest digest, final MeasureDesc measureDesc) {
        //is raw query
        if (!digest.isRawQuery)
            return null;

        TblColRef rawColumn = getRawColumn(measureDesc.getFunction());
        if (!digest.allColumns.isEmpty() && !digest.allColumns.contains(rawColumn)) {
            return null;
        }

        unmatchedAggregations.remove(measureDesc.getFunction());

        //contain one raw measure : cost * 0.9
        return new CapabilityResult.CapabilityInfluence() {
            @Override
            public double suggestCostMultiplier() {
                return 0.9;
            }

            @Override
            public MeasureDesc getInvolvedMeasure() {
                return measureDesc;
            }
        };
    }

    @Override
    public boolean needRewrite() {
        return false;
    }

    public void adjustSqlDigest(List<MeasureDesc> measureDescs, SQLDigest sqlDigest) {

        if (sqlDigest.isRawQuery) {
            for (MeasureDesc measureDesc : measureDescs) {
                if (!sqlDigest.involvedMeasure.contains(measureDesc)) {
                    continue;
                }
                TblColRef col = this.getRawColumn(measureDesc.getFunction());
                ParameterDesc colParameter = ParameterDesc.newInstance(col);
                FunctionDesc rawFunc = FunctionDesc.newInstance("RAW", Lists.newArrayList(colParameter), null);

                if (sqlDigest.allColumns.contains(col)) {
                    if (measureDesc.getFunction().equals(rawFunc)) {
                        FunctionDesc sumFunc = FunctionDesc.newInstance("SUM", Lists.newArrayList(colParameter), null);
                        sqlDigest.aggregations.remove(sumFunc);
                        sqlDigest.aggregations.add(rawFunc);
                        logger.info("Add RAW measure on column " + col);
                    }
                    if (!sqlDigest.metricColumns.contains(col)) {
                        sqlDigest.metricColumns.add(col);
                    }
                }
            }
        }
    }

    @Override
    public boolean needAdvancedTupleFilling() {
        return true;
    }

    @Override
    public void fillTupleSimply(Tuple tuple, int indexInTuple, Object measureValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IAdvMeasureFiller getAdvancedTupleFiller(FunctionDesc function, TupleInfo tupleInfo,
            Map<TblColRef, Dictionary<String>> dictionaryMap) {
        final TblColRef literalCol = getRawColumn(function);
        final Dictionary<String> rawColDict = dictionaryMap.get(literalCol);
        final int literalTupleIdx = tupleInfo.hasColumn(literalCol) ? tupleInfo.getColumnIndex(literalCol) : -1;

        return new IAdvMeasureFiller() {
            private List<ByteArray> rawList;
            private Iterator<ByteArray> rawIterator;
            private int expectRow = 0;

            @SuppressWarnings("unchecked")
            @Override
            public void reload(Object measureValue) {
                this.rawList = (List<ByteArray>) measureValue;
                this.rawIterator = rawList.iterator();
                this.expectRow = 0;
            }

            @Override
            public int getNumOfRows() {
                return rawList.size();
            }

            @Override
            public void fillTuple(Tuple tuple, int row) {
                if (expectRow++ != row)
                    throw new IllegalStateException();

                ByteArray raw = rawIterator.next();
                int key = BytesUtil.readUnsigned(raw.array(), raw.offset(), raw.length());
                String colValue = rawColDict.getValueFromId(key);
                tuple.setDimensionValue(literalTupleIdx, colValue);
            }
        };
    }

    private TblColRef getRawColumn(FunctionDesc functionDesc) {
        return functionDesc.getColRefs().get(0);
    }

    @Override
    public boolean onlyAggrInBaseCuboid() {
        return true;
    }
}

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

package org.apache.kylin.measure.topn;

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
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult.CapabilityInfluence;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class TopNMeasureType extends MeasureType<TopNCounter<ByteArray>> {

    private static final Logger logger = LoggerFactory.getLogger(TopNMeasureType.class);

    public static final String FUNC_TOP_N = "TOP_N";
    public static final String DATATYPE_TOPN = "topn";

    public static class Factory extends MeasureTypeFactory<TopNCounter<ByteArray>> {

        @Override
        public MeasureType<TopNCounter<ByteArray>> createMeasureType(String funcName, DataType dataType) {
            return new TopNMeasureType(funcName, dataType);
        }

        @Override
        public String getAggrFunctionName() {
            return FUNC_TOP_N;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_TOPN;
        }

        @Override
        public Class<? extends DataTypeSerializer<TopNCounter<ByteArray>>> getAggrDataTypeSerializer() {
            return TopNCounterSerializer.class;
        }
    }

    // ============================================================================

    private final DataType dataType;

    public TopNMeasureType(String funcName, DataType dataType) {
        // note at query parsing phase, the data type may be null, because only function and parameters are known
        this.dataType = dataType;
    }

    public void validate(FunctionDesc functionDesc) throws IllegalArgumentException {
        validate(functionDesc.getExpression(), functionDesc.getReturnDataType(), true);
    }

    private void validate(String funcName, DataType dataType, boolean checkDataType) {
        if (FUNC_TOP_N.equals(funcName) == false)
            throw new IllegalArgumentException();

        if (DATATYPE_TOPN.equals(dataType.getName()) == false)
            throw new IllegalArgumentException();

        if (dataType.getPrecision() < 1 || dataType.getPrecision() > 5000)
            throw new IllegalArgumentException();
    }

    @Override
    public boolean isMemoryHungry() {
        return true;
    }

    @Override
    public MeasureIngester<TopNCounter<ByteArray>> newIngester() {
        return new MeasureIngester<TopNCounter<ByteArray>>() {
            @Override
            public TopNCounter<ByteArray> valueOf(String[] values, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
                if (values.length != 2)
                    throw new IllegalArgumentException();

                double counter = values[0] == null ? 0 : Double.parseDouble(values[0]);
                String literal = values[1];

                // encode literal using dictionary
                TblColRef literalCol = getTopNLiteralColumn(measureDesc.getFunction());
                Dictionary<String> dictionary = dictionaryMap.get(literalCol);
                int keyEncodedValue = dictionary.getIdFromValue(literal);

                ByteArray key = new ByteArray(dictionary.getSizeOfId());
                BytesUtil.writeUnsigned(keyEncodedValue, key.array(), 0, dictionary.getSizeOfId());

                TopNCounter<ByteArray> topNCounter = new TopNCounter<ByteArray>(dataType.getPrecision() * TopNCounter.EXTRA_SPACE_RATE);
                topNCounter.offer(key, counter);
                return topNCounter;
            }

            @Override
            public TopNCounter<ByteArray> reEncodeDictionary(TopNCounter<ByteArray> value, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> oldDicts, Map<TblColRef, Dictionary<String>> newDicts) {
                TopNCounter<ByteArray> topNCounter = value;

                TblColRef colRef = getTopNLiteralColumn(measureDesc.getFunction());
                Dictionary<String> sourceDict = oldDicts.get(colRef);
                Dictionary<String> mergedDict = newDicts.get(colRef);

                int topNSize = topNCounter.size();
                byte[] newIdBuf = new byte[topNSize * mergedDict.getSizeOfId()];
                byte[] literal = new byte[sourceDict.getSizeOfValue()];

                int bufOffset = 0;
                for (Counter<ByteArray> c : topNCounter) {
                    int oldId = BytesUtil.readUnsigned(c.getItem().array(), c.getItem().offset(), c.getItem().length());
                    int newId;
                    int size = sourceDict.getValueBytesFromId(oldId, literal, 0);
                    if (size < 0) {
                        newId = mergedDict.nullId();
                    } else {
                        newId = mergedDict.getIdFromValueBytes(literal, 0, size);
                    }

                    BytesUtil.writeUnsigned(newId, newIdBuf, bufOffset, mergedDict.getSizeOfId());
                    c.getItem().set(newIdBuf, bufOffset, mergedDict.getSizeOfId());
                    bufOffset += mergedDict.getSizeOfId();
                }
                return value;
            }
        };
    }

    @Override
    public MeasureAggregator<TopNCounter<ByteArray>> newAggregator() {
        return new TopNAggregator();
    }

    @Override
    public List<TblColRef> getColumnsNeedDictionary(FunctionDesc functionDesc) {
        TblColRef literalCol = functionDesc.getParameter().getColRefs().get(1);
        return Collections.singletonList(literalCol);
    }

    @Override
    public CapabilityInfluence influenceCapabilityCheck(Collection<TblColRef> unmatchedDimensions, Collection<FunctionDesc> unmatchedAggregations, SQLDigest digest, MeasureDesc topN) {
        // TopN measure can (and only can) provide one numeric measure and one literal dimension
        // e.g. select seller, sum(gmv) from ... group by seller order by 2 desc limit 100

        // check digest requires only one measure
        if (digest.aggregations.size() != 1)
            return null;

        // the measure function must be SUM
        FunctionDesc onlyFunction = digest.aggregations.iterator().next();
        if (isTopNCompatibleSum(topN.getFunction(), onlyFunction) == false)
            return null;

        TblColRef literalCol = getTopNLiteralColumn(topN.getFunction());
        if (unmatchedDimensions.contains(literalCol) == false)
            return null;
        if (digest.groupbyColumns.contains(literalCol) == false)
            return null;

        unmatchedDimensions.remove(literalCol);
        unmatchedAggregations.remove(onlyFunction);
        return new CapabilityInfluence() {
            @Override
            public double suggestCostMultiplier() {
                return 0.3; // make sure TopN get ahead of other matched realizations
            }
        };
    }

    private boolean isTopNCompatibleSum(FunctionDesc topN, FunctionDesc sum) {
        if (sum == null)
            return false;

        if (!isTopN(topN) || !sum.isSum())
            return false;

        if (sum.getParameter().getColRefs().isEmpty())
            return false;

        TblColRef sumCol = sum.getParameter().getColRefs().get(0);
        TblColRef topnNumCol = getTopNNumericColumn(topN);
        return sumCol.equals(topnNumCol);
    }

    @Override
    public boolean needRewrite() {
        return false;
    }

    @Override
    public Class<?> getRewriteCalciteAggrFunctionClass() {
        return null;
    }

    @Override
    public void adjustSqlDigest(MeasureDesc measureDesc, SQLDigest sqlDigest) {
        FunctionDesc topnFunc = measureDesc.getFunction();
        TblColRef topnLiteralCol = getTopNLiteralColumn(topnFunc);

        if (sqlDigest.groupbyColumns.contains(topnLiteralCol) == false)
            return;

        if (sqlDigest.aggregations.size() != 1) {
            throw new IllegalStateException("When query with topN, only one metrics is allowed.");
        }

        FunctionDesc origFunc = sqlDigest.aggregations.iterator().next();
        if (origFunc.isSum() == false) {
            throw new IllegalStateException("When query with topN, only SUM function is allowed.");
        }

        sqlDigest.aggregations = Lists.newArrayList(topnFunc);
        sqlDigest.groupbyColumns.remove(topnLiteralCol);
        sqlDigest.metricColumns.add(topnLiteralCol);
        logger.info("Rewrite function " + origFunc + " to " + topnFunc);
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
    public IAdvMeasureFiller getAdvancedTupleFiller(FunctionDesc function, TupleInfo tupleInfo, Map<TblColRef, Dictionary<String>> dictionaryMap) {
        final TblColRef literalCol = getTopNLiteralColumn(function);
        final TblColRef numericCol = getTopNNumericColumn(function);
        final Dictionary<String> topNColDict = dictionaryMap.get(literalCol);
        final int literalTupleIdx = tupleInfo.hasColumn(literalCol) ? tupleInfo.getColumnIndex(literalCol) : -1;
        // for TopN, the aggr must be SUM, so the number fill into the column position (without rewrite)
        final int numericTupleIdx = tupleInfo.hasColumn(numericCol) ? tupleInfo.getColumnIndex(numericCol) : -1;

        return new IAdvMeasureFiller() {
            private TopNCounter<ByteArray> topNCounter;
            private Iterator<Counter<ByteArray>> topNCounterIterator;
            private int expectRow = 0;

            @SuppressWarnings("unchecked")
            @Override
            public void reload(Object measureValue) {
                this.topNCounter = (TopNCounter<ByteArray>) measureValue;
                this.topNCounterIterator = topNCounter.iterator();
                this.expectRow = 0;
            }

            @Override
            public int getNumOfRows() {
                return topNCounter.size();
            }

            @Override
            public void fillTuplle(Tuple tuple, int row) {
                if (expectRow++ != row)
                    throw new IllegalStateException();

                Counter<ByteArray> counter = topNCounterIterator.next();
                int key = BytesUtil.readUnsigned(counter.getItem().array(), counter.getItem().offset(), counter.getItem().length());
                String colValue = topNColDict.getValueFromId(key);
                tuple.setDimensionValue(literalTupleIdx, colValue);
                tuple.setMeasureValue(numericTupleIdx, counter.getCount());
            }
        };
    }

    private TblColRef getTopNNumericColumn(FunctionDesc functionDesc) {
        return functionDesc.getParameter().getColRefs().get(0);
    }

    private TblColRef getTopNLiteralColumn(FunctionDesc functionDesc) {
        return functionDesc.getParameter().getColRefs().get(1);
    }

    private boolean isTopN(FunctionDesc functionDesc) {
        return FUNC_TOP_N.equalsIgnoreCase(functionDesc.getExpression());
    }
}

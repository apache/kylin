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

import org.apache.kylin.common.topn.Counter;
import org.apache.kylin.common.topn.TopNCounter;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.hllc.HLLCSerializer;
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

public class TopNMeasureType extends MeasureType {

    private static final Logger logger = LoggerFactory.getLogger(TopNMeasureType.class);

    private final DataType dataType;

    public TopNMeasureType(DataType dataType) {
        if ("topn".equals(dataType.getName()) == false)
            throw new IllegalArgumentException();

        this.dataType = dataType;

        if (this.dataType.getPrecision() < 1 || this.dataType.getPrecision() > 1000)
            throw new IllegalArgumentException("TopN precision must be between 1 and 1000");
    }

    @Override
    public DataType getAggregationDataType() {
        return dataType;
    }

    @Override
    public Class<? extends DataTypeSerializer<?>> getAggregationDataSeralizer() {
        return HLLCSerializer.class;
    }

    @Override
    public void validate(MeasureDesc measureDesc) throws IllegalArgumentException {
        // TODO Auto-generated method stub

    }

    @SuppressWarnings("rawtypes")
    @Override
    public MeasureIngester<?> newIngester() {
        return new MeasureIngester<TopNCounter>() {
            @Override
            public TopNCounter valueOf(String[] values, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
                if (values.length != 2)
                    throw new IllegalArgumentException();

                double counter = values[0] == null ? 0 : Double.parseDouble(values[0]);
                String literal = values[1];

                // encode literal using dictionary
                TblColRef literalCol = measureDesc.getFunction().getTopNLiteralColumn();
                Dictionary<String> dictionary = dictionaryMap.get(literalCol);
                int keyEncodedValue = dictionary.getIdFromValue(literal);

                ByteArray key = new ByteArray(dictionary.getSizeOfId());
                BytesUtil.writeUnsigned(keyEncodedValue, key.array(), 0, dictionary.getSizeOfId());

                TopNCounter<ByteArray> topNCounter = new TopNCounter<ByteArray>(dataType.getPrecision() * TopNCounter.EXTRA_SPACE_RATE);
                topNCounter.offer(key, counter);
                return topNCounter;
            }

            @SuppressWarnings("unchecked")
            @Override
            public TopNCounter reEncodeDictionary(TopNCounter value, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> oldDicts, Map<TblColRef, Dictionary<String>> newDicts) {
                TopNCounter<ByteArray> topNCounter = (TopNCounter<ByteArray>) value;

                TblColRef colRef = measureDesc.getFunction().getTopNLiteralColumn();
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
    public MeasureAggregator<?> newAggregator() {
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
        if (onlyFunction.isSum() == false)
            return null;

        TblColRef literalCol = topN.getFunction().getTopNLiteralColumn();
        if (unmatchedDimensions.contains(literalCol) && topN.getFunction().isTopNCompatibleSum(onlyFunction)) {
            unmatchedDimensions.remove(literalCol);
            unmatchedAggregations.remove(onlyFunction);
            return new CapabilityInfluence() {
                @Override
                public double suggestCostMultiplier() {
                    return 0.3; // make sure TopN get ahead of other matched realizations
                }
            };
        } else
            return null;
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
    public void beforeStorageQuery(MeasureDesc measureDesc, SQLDigest sqlDigest) {
        FunctionDesc topnFunc = measureDesc.getFunction();
        TblColRef topnLiteralCol = topnFunc.getTopNLiteralColumn();

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
                int key = BytesUtil.readUnsigned(counter.getItem().array(), 0, counter.getItem().array().length);
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
}

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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.dimension.DateDimEnc;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.DimensionEncodingFactory;
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

    public static final String CONFIG_ENCODING_PREFIX = "topn.encoding.";
    public static final String CONFIG_ENCODING_VERSION_PREFIX = "topn.encoding_version.";
    public static final String CONFIG_AGG = "topn.aggregation";
    public static final String CONFIG_ORDER = "topn.order";

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
        if (!FUNC_TOP_N.equals(funcName))
            throw new IllegalArgumentException();

        if (!DATATYPE_TOPN.equals(dataType.getName()))
            throw new IllegalArgumentException();

        if (dataType.getPrecision() < 1 || dataType.getPrecision() > 10000)
            throw new IllegalArgumentException();
    }

    public static String getRewriteName(FunctionDesc func) {
        return getTopnInternalMeasure(func).getRewriteFieldName();
    }

    public static FunctionDesc getTopnInternalMeasure(FunctionDesc func) {
        if (func.getParameters().get(0).isColumnType()) {
            return FunctionDesc.newInstance(FunctionDesc.FUNC_SUM,
                    Lists.newArrayList(ParameterDesc.newInstance(func.getParameters().get(0).getColRef())), null);
        } else {
            return FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT, Lists.newArrayList(ParameterDesc.newInstance("1")),
                    null);
        }
    }

    @Override
    public boolean isMemoryHungry() {
        return true;
    }

    @Override
    public MeasureIngester<TopNCounter<ByteArray>> newIngester() {
        return new MeasureIngester<TopNCounter<ByteArray>>() {

            private DimensionEncoding[] dimensionEncodings = null;
            private List<TblColRef> literalCols = null;
            private int keyLength = 0;

            private DimensionEncoding[] newDimensionEncodings = null;
            private int newKeyLength = 0;
            private boolean needReEncode = true;

            @Override
            public TopNCounter<ByteArray> valueOf(String[] values, MeasureDesc measureDesc,
                    Map<TblColRef, Dictionary<String>> dictionaryMap) {
                double counter = values[0] == null ? 0 : Double.parseDouble(values[0]);

                if (dimensionEncodings == null) {
                    literalCols = getTopNLiteralColumn(measureDesc.getFunction());
                    dimensionEncodings = getDimensionEncodings(measureDesc.getFunction(), literalCols, dictionaryMap);
                    for (DimensionEncoding encoding : dimensionEncodings) {
                        keyLength += encoding.getLengthOfEncoding();
                    }

                    if (values.length != (literalCols.size() + 1)) {
                        throw new IllegalArgumentException();
                    }
                }

                final ByteArray key = new ByteArray(keyLength);
                int offset = 0;
                for (int i = 0; i < dimensionEncodings.length; i++) {
                    if (values[i + 1] == null) {
                        Arrays.fill(key.array(), offset, offset + dimensionEncodings[i].getLengthOfEncoding(),
                                DimensionEncoding.NULL);
                    } else {
                        dimensionEncodings[i].encode(values[i + 1], key.array(), offset);
                    }
                    offset += dimensionEncodings[i].getLengthOfEncoding();
                }

                TopNCounter<ByteArray> topNCounter = new TopNCounter<ByteArray>(
                        dataType.getPrecision() * TopNCounter.EXTRA_SPACE_RATE);
                topNCounter.offer(key, counter);
                return topNCounter;
            }

            @Override
            public TopNCounter<ByteArray> reEncodeDictionary(TopNCounter<ByteArray> value, MeasureDesc measureDesc,
                    Map<TblColRef, Dictionary<String>> oldDicts, Map<TblColRef, Dictionary<String>> newDicts) {
                TopNCounter<ByteArray> topNCounter = value;

                if (newDimensionEncodings == null) {
                    literalCols = getTopNLiteralColumn(measureDesc.getFunction());
                    dimensionEncodings = getDimensionEncodings(measureDesc.getFunction(), literalCols, oldDicts);
                    keyLength = 0;
                    boolean hasDictEncoding = false;
                    for (DimensionEncoding encoding : dimensionEncodings) {
                        keyLength += encoding.getLengthOfEncoding();
                        if (encoding instanceof DictionaryDimEnc) {
                            hasDictEncoding = true;
                        }
                    }

                    newDimensionEncodings = getDimensionEncodings(measureDesc.getFunction(), literalCols, newDicts);
                    newKeyLength = 0;
                    for (DimensionEncoding encoding : newDimensionEncodings) {
                        newKeyLength += encoding.getLengthOfEncoding();
                    }

                    needReEncode = hasDictEncoding;
                }

                if (!needReEncode) {
                    // no need re-encode
                    return topNCounter;
                }

                int topNSize = topNCounter.size();
                byte[] newIdBuf = new byte[topNSize * newKeyLength];

                int bufOffset = 0;
                for (Counter<ByteArray> c : topNCounter) {
                    int offset = c.getItem().offset();
                    int innerBuffOffset = 0;
                    for (int i = 0; i < dimensionEncodings.length; i++) {
                        String dimValue = dimensionEncodings[i].decode(c.getItem().array(), offset,
                                dimensionEncodings[i].getLengthOfEncoding());
                        newDimensionEncodings[i].encode(dimValue, newIdBuf, bufOffset + innerBuffOffset);
                        innerBuffOffset += newDimensionEncodings[i].getLengthOfEncoding();
                        offset += dimensionEncodings[i].getLengthOfEncoding();
                    }

                    c.getItem().reset(newIdBuf, bufOffset, newKeyLength);
                    bufOffset += newKeyLength;
                }
                return topNCounter;
            }
        };
    }

    @Override
    public MeasureAggregator<TopNCounter<ByteArray>> newAggregator() {
        return new TopNAggregator();
    }

    @Override
    public List<TblColRef> getColumnsNeedDictionary(FunctionDesc functionDesc) {
        List<TblColRef> columnsNeedDict = Lists.newArrayList();
        List<TblColRef> allCols = functionDesc.getColRefs();
        int start = (functionDesc.getParameters().get(0).isColumnType()) ? 1 : 0;
        for (int i = start; i < allCols.size(); i++) {
            TblColRef tblColRef = allCols.get(i);
            String encoding = getEncoding(functionDesc, tblColRef).getFirst();
            if (StringUtils.isEmpty(encoding) || DictionaryDimEnc.ENCODING_NAME.equals(encoding)) {
                columnsNeedDict.add(tblColRef);
            }
        }

        return columnsNeedDict;
    }

    @Override
    public CapabilityInfluence influenceCapabilityCheck(Collection<TblColRef> unmatchedDimensions,
            Collection<FunctionDesc> unmatchedAggregations, SQLDigest digest, final MeasureDesc topN) {
        // TopN measure can (and only can) provide one numeric measure and one literal dimension
        // e.g. select seller, sum(gmv) from ... group by seller order by 2 desc limit 100

        List<TblColRef> literalCol = getTopNLiteralColumn(topN.getFunction());
        for (TblColRef colRef : literalCol) {
            if (digest.filterColumns.contains(colRef)) {
                // doesn't allow filtering by topn literal column
                return null;
            }
        }

        if (!digest.groupbyColumns.containsAll(literalCol) || !literalCol.containsAll(digest.groupbyColumns))
            return null;

        // check digest requires only one measure
        if (digest.aggregations.size() == 1) {

            // the measure function must be SUM
            FunctionDesc onlyFunction = digest.aggregations.iterator().next();
            if (!isTopNCompatibleSum(topN.getFunction(), onlyFunction))
                return null;

            if (!checkOrderByAndLimit(digest, topN.getFunction().getReturnDataType().getPrecision()))
                return null;

            unmatchedDimensions.removeAll(literalCol);
            unmatchedAggregations.remove(onlyFunction);
            return new CapabilityInfluence() {
                @Override
                public double suggestCostMultiplier() {
                    return 0.3; // make sure TopN get ahead of other matched realizations
                }

                @Override
                public MeasureDesc getInvolvedMeasure() {
                    return topN;
                }
            };
        }

        return null;
    }

    private boolean checkOrderByAndLimit(SQLDigest digest, int topNPrecision) {
        if (digest.limit > topNPrecision) {
            return false;
        }

        // Top N style query can only has one order by field and must be descending order.
        if (digest.sortColumns.size() != 1 || digest.sortOrders.get(0) != SQLDigest.OrderEnum.DESCENDING) {
            return false;
        }
        TblColRef sortCol = digest.sortColumns.get(0);
        // String database = sortCol.getColumnDesc().getTable().getDatabase();

        for (FunctionDesc agg : digest.aggregations) {
            //  if (!agg.getParameters().get(0).getColRef().getColumnDesc().getTable().getDatabase().equals(database)) {
            //      continue;
            //  }
            if (sortCol.getName().equals(agg.getRewriteFieldName())) {
                return true;
            }

            if (sortCol.isInnerColumn() && sortCol.getOperator() != null
                    && sortCol.getOperator().getName().equals(agg.getExpression())
                    && sortCol.getOperands().equals(agg.getColRefs())) {
                return true;
            }
        }
        return false;
    }

    private boolean isTopNCompatibleSum(FunctionDesc topN, FunctionDesc sum) {
        if (sum == null)
            return false;

        if (!isTopN(topN))
            return false;

        TblColRef topnNumCol = getTopNNumericColumn(topN);

        if (topnNumCol == null) {
            if (sum.isCount())
                return true;

            return false;
        }

        if (!sum.isSum())
            return false;

        if (CollectionUtils.isEmpty(sum.getParameters()) || CollectionUtils.isEmpty(sum.getColRefs()))
            return false;

        TblColRef sumCol = sum.getColRefs().get(0);
        return sumCol.equals(topnNumCol);
    }

    @Override
    public boolean needRewrite() {
        return true;
    }

    @Override
    public void adjustSqlDigest(MeasureDesc involvedMeasure, SQLDigest sqlDigest) {
        if (sqlDigest.aggregations.size() > 1) {
            return;
        }

        FunctionDesc topnFunc = involvedMeasure.getFunction();
        List<TblColRef> topnLiteralCol = getTopNLiteralColumn(topnFunc);

        if (!sqlDigest.groupbyColumns.containsAll(topnLiteralCol)
                || !topnLiteralCol.containsAll(sqlDigest.groupbyColumns)) {
            return;
        }

        if (!sqlDigest.aggregations.isEmpty()) {
            FunctionDesc origFunc = sqlDigest.aggregations.iterator().next();
            if (!origFunc.isSum() && !origFunc.isCount()) {
                logger.warn("When query with topN, only SUM/Count function is allowed.");
                return;
            }

            if (!isTopNCompatibleSum(involvedMeasure.getFunction(), origFunc)) {
                return;
            }

            logger.info("Rewrite function " + origFunc + " to " + topnFunc);
        }

        sqlDigest.aggregations = Lists.newArrayList(topnFunc);
        sqlDigest.groupbyColumns.removeAll(topnLiteralCol);
        sqlDigest.metricColumns.addAll(topnLiteralCol);
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
        final List<TblColRef> literalCols = getTopNLiteralColumn(function);
        final TblColRef numericCol = getTopNNumericColumn(function);
        final int[] literalTupleIdx = new int[literalCols.size()];
        final DimensionEncoding[] dimensionEncodings = getDimensionEncodings(function, literalCols, dictionaryMap);
        for (int i = 0; i < literalCols.size(); i++) {
            TblColRef colRef = literalCols.get(i);
            literalTupleIdx[i] = tupleInfo.hasColumn(colRef) ? tupleInfo.getColumnIndex(colRef) : -1;
        }

        // for TopN, the aggr must be SUM
        final int numericTupleIdx;
        if (numericCol != null) {
            FunctionDesc sumFunc = FunctionDesc.newInstance(FunctionDesc.FUNC_SUM,
                    Lists.newArrayList(ParameterDesc.newInstance(numericCol)), numericCol.getType().toString());
            String sumFieldName = sumFunc.getRewriteFieldName();
            numericTupleIdx = tupleInfo.hasField(sumFieldName) ? tupleInfo.getFieldIndex(sumFieldName) : -1;
        } else {
            FunctionDesc countFunction = FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT,
                    Lists.newArrayList(ParameterDesc.newInstance("1")), "bigint");
            numericTupleIdx = tupleInfo.getFieldIndex(countFunction.getRewriteFieldName());
        }
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
            public void fillTuple(Tuple tuple, int row) {
                if (expectRow++ != row)
                    throw new IllegalStateException();

                Counter<ByteArray> counter = topNCounterIterator.next();
                int offset = counter.getItem().offset();
                for (int i = 0; i < dimensionEncodings.length; i++) {
                    String colValue = dimensionEncodings[i].decode(counter.getItem().array(), offset,
                            dimensionEncodings[i].getLengthOfEncoding());
                    tuple.setDimensionValue(literalTupleIdx[i], colValue);
                    offset += dimensionEncodings[i].getLengthOfEncoding();
                }
                tuple.setMeasureValue(numericTupleIdx, counter.getCount());
            }
        };
    }

    private static DimensionEncoding[] getDimensionEncodings(FunctionDesc function, List<TblColRef> literalCols,
            Map<TblColRef, Dictionary<String>> dictionaryMap) {
        final DimensionEncoding[] dimensionEncodings = new DimensionEncoding[literalCols.size()];
        for (int i = 0; i < literalCols.size(); i++) {
            TblColRef colRef = literalCols.get(i);

            Pair<String, String> topNEncoding = TopNMeasureType.getEncoding(function, colRef);
            String encoding = topNEncoding.getFirst();
            String encodingVersionStr = topNEncoding.getSecond();
            if (StringUtils.isEmpty(encoding) || DictionaryDimEnc.ENCODING_NAME.equals(encoding)) {
                dimensionEncodings[i] = new DictionaryDimEnc(dictionaryMap.get(colRef));
            } else {
                int encodingVersion = 1;
                if (!StringUtils.isEmpty(encodingVersionStr)) {
                    try {
                        encodingVersion = Integer.parseInt(encodingVersionStr);
                    } catch (NumberFormatException e) {
                        throw new RuntimeException(TopNMeasureType.CONFIG_ENCODING_VERSION_PREFIX + colRef.getName()
                                + " has to be an integer");
                    }
                }
                Object[] encodingConf = DimensionEncoding.parseEncodingConf(encoding);
                String encodingName = (String) encodingConf[0];
                String[] encodingArgs = (String[]) encodingConf[1];

                encodingArgs = DateDimEnc.replaceEncodingArgs(encoding, encodingArgs, encodingName,
                        literalCols.get(i).getType());

                dimensionEncodings[i] = DimensionEncodingFactory.create(encodingName, encodingArgs, encodingVersion);
            }
        }

        return dimensionEncodings;
    }

    private TblColRef getTopNNumericColumn(FunctionDesc functionDesc) {
        if (functionDesc.getParameters().get(0).isColumnType()) {
            return functionDesc.getColRefs().get(0);
        }
        return null;
    }

    private List<TblColRef> getTopNLiteralColumn(FunctionDesc functionDesc) {
        List<TblColRef> allColumns = functionDesc.getColRefs();
        if (!functionDesc.getParameters().get(0).isColumnType()) {
            return allColumns;
        }
        return allColumns.subList(1, allColumns.size());
    }

    private boolean isTopN(FunctionDesc functionDesc) {
        return FUNC_TOP_N.equalsIgnoreCase(functionDesc.getExpression());
    }

    /**
     * Get the encoding name and version for the given col from Measure FunctionDesc
     * @param functionDesc
     * @param tblColRef
     * @return a pair of the encoding name and encoding version
     */
    public static final Pair<String, String> getEncoding(FunctionDesc functionDesc, TblColRef tblColRef) {
        String encoding = functionDesc.getConfiguration().get(CONFIG_ENCODING_PREFIX + tblColRef.getIdentity());
        String encodingVersion = functionDesc.getConfiguration()
                .get(CONFIG_ENCODING_VERSION_PREFIX + tblColRef.getIdentity());
        if (StringUtils.isEmpty(encoding)) {
            // for backward compatibility
            encoding = functionDesc.getConfiguration().get(CONFIG_ENCODING_PREFIX + tblColRef.getName());
            encodingVersion = functionDesc.getConfiguration().get(CONFIG_ENCODING_VERSION_PREFIX + tblColRef.getName());
        }

        return new Pair<>(encoding, encodingVersion);
    }

    /**
     *  update return type scale with the estimated key length
     */
    public static void fixMeasureReturnType(MeasureDesc measureDesc) {
        Map<String, String> configuration = measureDesc.getFunction().getConfiguration();
        List<ParameterDesc> parameters = measureDesc.getFunction().getParameters();
        int keyLength = 0;

        for (ParameterDesc parameter : parameters.subList(1, parameters.size())) {
            String encoding = configuration.get(TopNMeasureType.CONFIG_ENCODING_PREFIX + parameter.getValue());
            String encodingVersionStr = configuration
                    .get(TopNMeasureType.CONFIG_ENCODING_VERSION_PREFIX + parameter.getValue());
            if (StringUtils.isEmpty(encoding) || DictionaryDimEnc.ENCODING_NAME.equals(encoding)) {
                keyLength += DictionaryDimEnc.MAX_ENCODING_LENGTH; // estimation for dict encoding
            } else {
                // non-dict encoding
                int encodingVersion = 1;
                if (!StringUtils.isEmpty(encodingVersionStr)) {
                    try {
                        encodingVersion = Integer.parseInt(encodingVersionStr);
                    } catch (NumberFormatException e) {
                        throw new RuntimeException("invalid encoding version: " + encodingVersionStr);
                    }
                }
                Object[] encodingConf = DimensionEncoding.parseEncodingConf(encoding);
                DimensionEncoding dimensionEncoding = DimensionEncodingFactory.create((String) encodingConf[0],
                        (String[]) encodingConf[1], encodingVersion);
                keyLength += dimensionEncoding.getLengthOfEncoding();
            }
        }

        DataType returnType = DataType.getType(measureDesc.getFunction().getReturnType());
        DataType newReturnType = new DataType(returnType.getName(), returnType.getPrecision(), keyLength);
        measureDesc.getFunction().setReturnType(newReturnType.toString());
    }
}

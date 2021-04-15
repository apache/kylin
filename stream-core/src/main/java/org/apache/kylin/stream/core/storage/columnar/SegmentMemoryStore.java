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

package org.apache.kylin.stream.core.storage.columnar;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureAggregators;
import org.apache.kylin.measure.topn.TopNAggregator;
import org.apache.kylin.metadata.filter.StringCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;
import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.query.IStreamingGTSearcher;
import org.apache.kylin.stream.core.query.IStreamingSearchResult;
import org.apache.kylin.stream.core.query.ResponseResultSchema;
import org.apache.kylin.stream.core.query.ResultCollector;
import org.apache.kylin.stream.core.query.StreamingBuiltInFunctionTransformer;
import org.apache.kylin.stream.core.query.StreamingQueryProfile;
import org.apache.kylin.stream.core.query.StreamingSearchContext;
import org.apache.kylin.stream.core.storage.Record;
import org.apache.kylin.stream.core.storage.columnar.ParsedStreamingCubeInfo.CuboidInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class SegmentMemoryStore implements IStreamingGTSearcher {
    private static Logger logger = LoggerFactory.getLogger(SegmentMemoryStore.class);

    protected final ParsedStreamingCubeInfo parsedStreamingCubeInfo;
    protected final String segmentName;

    private volatile Map<CuboidInfo, ConcurrentMap<String[], MeasureAggregator[]>> cuboidsAggBufMap;
    private volatile ConcurrentMap<String[], MeasureAggregator[]> basicCuboidAggBufMap;

    private volatile AtomicInteger rowCount = new AtomicInteger();
    private volatile AtomicInteger originRowCount = new AtomicInteger();
    private long minEventTime = Long.MAX_VALUE;
    private long maxEventTime = 0;

    private Map<TblColRef, Dictionary<String>> dictionaryMap;

    public void setDictionaryMap(Map<TblColRef, Dictionary<String>> dictionaryMap) {
        this.dictionaryMap = dictionaryMap;
    }

    public SegmentMemoryStore(ParsedStreamingCubeInfo parsedStreamingCubeInfo, String segmentName) {
        this.parsedStreamingCubeInfo = parsedStreamingCubeInfo;
        this.segmentName = segmentName;

        this.basicCuboidAggBufMap = new ConcurrentSkipListMap<>(StringArrayComparator.INSTANCE);
        List<CuboidInfo> additionalCuboids = parsedStreamingCubeInfo.getAdditionalCuboidsToBuild();
        if (additionalCuboids != null && !additionalCuboids.isEmpty()) {
            this.cuboidsAggBufMap = new ConcurrentHashMap<>(additionalCuboids.size());
            for (CuboidInfo cuboidInfo : additionalCuboids) {
                cuboidsAggBufMap.put(cuboidInfo, new ConcurrentSkipListMap<>(
                        StringArrayComparator.INSTANCE));
            }
        }
    }

    public int index(StreamingMessage event) {
        long eventTime = event.getTimestamp();
        if (eventTime < minEventTime) {
            minEventTime = eventTime;
        }
        if (eventTime > maxEventTime) {
            maxEventTime = eventTime;
        }
        List<String> row = event.getData();
        parsedStreamingCubeInfo.resetAggrs();
        String[] basicCuboidDimensions = buildBasicCuboidKey(row);
        Object[] metricsValues = buildValue(row);
        aggregate(basicCuboidAggBufMap, basicCuboidDimensions, metricsValues);
        if (cuboidsAggBufMap != null) {
            for (Entry<CuboidInfo, ConcurrentMap<String[], MeasureAggregator[]>> cuboidAggEntry : cuboidsAggBufMap
                    .entrySet()) {
                CuboidInfo cuboidInfo = cuboidAggEntry.getKey();
                ConcurrentMap<String[], MeasureAggregator[]> cuboidAggMap = cuboidAggEntry.getValue();
                String[] cuboidDimensions = buildCuboidKey(cuboidInfo, row);
                aggregate(cuboidAggMap, cuboidDimensions, metricsValues);
            }
        }
        originRowCount.incrementAndGet();
        return rowCount.get();
    }

    protected String[] buildBasicCuboidKey(List<String> row) {
        String[] key = new String[parsedStreamingCubeInfo.dimCount];
        for (int i = 0; i < parsedStreamingCubeInfo.dimCount; i++) {
            //The built key order is based on the row key column order.
            key[i] = row.get(parsedStreamingCubeInfo.intermediateTableDesc.getRowKeyColumnIndexes()[i]);
        }
        return key;
    }

    protected String[] buildCuboidKey(CuboidInfo cuboidInfo, List<String> row) {
        int[] columnsIndex = cuboidInfo.getColumnsIndex();
        String[] key = new String[columnsIndex.length];
        for (int i = 0; i < key.length; i++) {
            //The built key order is based on the row key column order.
            key[i] = row.get(columnsIndex[i]);
        }
        return key;
    }

    protected Object[] buildValue(List<String> row) {
        Object[] values = new Object[parsedStreamingCubeInfo.measureDescs.length];
        for (int i = 0; i < parsedStreamingCubeInfo.measureDescs.length; i++) {
            values[i] = buildValueOf(i, row);
        }
        return values;
    }

    private Object buildValueOf(int idxOfMeasure, List<String> row) {
        MeasureDesc measure = parsedStreamingCubeInfo.measureDescs[idxOfMeasure];
        FunctionDesc function = measure.getFunction();
        int[] colIdxOnFlatTable = parsedStreamingCubeInfo.intermediateTableDesc.getMeasureColumnIndexes()[idxOfMeasure];

        int paramCount = function.getParameterCount();
        String[] inputToMeasure = new String[paramCount];

        // pick up parameter values
        ParameterDesc param = function.getParameter();
        int paramColIdx = 0; // index among parameters of column type
        for (int i = 0; i < paramCount; i++, param = param.getNextParameter()) {
            String value;
            if (function.isCount()) {
                value = "1";
            } else if (param.isColumnType()) {
                value = row.get(colIdxOnFlatTable[paramColIdx++]);
            } else {
                value = param.getValue();
            }
            inputToMeasure[i] = value;
        }
        return parsedStreamingCubeInfo.measureIngesters[idxOfMeasure].valueOf(inputToMeasure, measure, dictionaryMap);
    }

    @SuppressWarnings("unchecked")
    private void aggregate(ConcurrentMap<String[], MeasureAggregator[]> cuboidAggBufMap, String[] dimensions,
            Object[] metricsValues) {
        MeasureAggregator[] aggrs = cuboidAggBufMap.get(dimensions);
        if (aggrs != null) {
            aggregateValues(aggrs, metricsValues);
        }

        if (aggrs == null) {
            MeasureAggregator[] newAggrs = newMetricsAggregators(parsedStreamingCubeInfo.metricsAggrFuncs);
            aggregateValues(newAggrs, metricsValues);
            aggrs = cuboidAggBufMap.putIfAbsent(dimensions, newAggrs);
            if (aggrs == null) {
                rowCount.incrementAndGet();
            } else {
                aggregateValues(aggrs, metricsValues);
            }
        }

    }

    private void aggregateValues(MeasureAggregator[] aggrs, Object[] metricsValues) {
        for (int i = 0; i < aggrs.length; i++) {
            synchronized (aggrs[i]) {
                aggrs[i].aggregate(metricsValues[i]);
            }
        }
    }

    private MeasureAggregator<?>[] newMetricsAggregators(String[] aggrFunctions) {
        MeasureAggregator<?>[] result = new MeasureAggregator[aggrFunctions.length];
        for (int i = 0; i < result.length; i++) {
            int col = parsedStreamingCubeInfo.dimCount + i;
            result[i] = MeasureAggregator.create(aggrFunctions[i], parsedStreamingCubeInfo.getAllDataTypes()[col]);
        }
        return result;
    }

    public int getRowCount() {
        return rowCount.get();
    }

    public int getOriginRowCount() {
        return originRowCount.get();
    }

    public ConcurrentMap<String[], MeasureAggregator[]> getBasicCuboidData() {
        return basicCuboidAggBufMap;
    }

    public Map<CuboidInfo, ConcurrentMap<String[], MeasureAggregator[]>> getAdditionalCuboidsData() {
        return cuboidsAggBufMap;
    }

    public ConcurrentMap<String[], MeasureAggregator[]> getCuboidData(long cuboidID) {
        if (cuboidID == parsedStreamingCubeInfo.basicCuboid.getId()) {
            return basicCuboidAggBufMap;
        } else {
            CuboidInfo cuboidInfo = new CuboidInfo(cuboidID);
            ConcurrentMap<String[], MeasureAggregator[]> result = cuboidsAggBufMap.get(cuboidInfo);
            if (result != null) {
                return result;
            }
            logger.warn("no in memory cuboid data find for cuboid:{}", cuboidID);
        }
        return basicCuboidAggBufMap;
    }

    public long getMinEventTime() {
        return minEventTime;
    }

    public long getMaxEventTime() {
        return maxEventTime;
    }

    @Override
    public void search(StreamingSearchContext searchContext, ResultCollector collector) throws IOException {
        ResponseResultSchema schema = searchContext.getRespResultSchema();
        TblColRef[] selectedDimensions = schema.getDimensions();
        FunctionDesc[] selectedMetrics = schema.getMetrics();

        collector.collectSearchResult(new AggregationBufferSearchResult(searchContext, selectedDimensions, selectedMetrics));
    }

    /**
     * AggregationBufferSearchResult to prepare the internal aggBufMap to IGTScanner for in-memory querying(filtering/aggregation)
     * The input aggBufMap state can be changed at any time, it must be a thread-safe map container for concurrent iteration.
     *
     */
    private class AggregationBufferSearchResult implements IStreamingSearchResult {
        private Map<String[], MeasureAggregator[]> aggBufMap;
        private int[] dimIndexes;
        private int[] metricsIndexes;
        private Map<TblColRef, Integer> dimColIdxMap;

        private TupleFilter filter;
        private int count = 0;
        private long scanCnt = 0;
        private long filterCnt = 0;

        private StreamingQueryProfile queryProfile;

        @SuppressWarnings("rawtypes")
        public AggregationBufferSearchResult(StreamingSearchContext searchRequest,
                                             TblColRef[] selectedDimensions, FunctionDesc[] selectedMetrics) {
            long hitCuboid = searchRequest.getHitCuboid();
            this.filter = searchRequest.getFilter();
            this.aggBufMap = getCuboidData(hitCuboid);
            this.dimIndexes = new int[selectedDimensions.length];
            this.metricsIndexes = new int[selectedMetrics.length];
            this.dimColIdxMap = Maps.newHashMap();

            CuboidInfo cuboidInfo = parsedStreamingCubeInfo.getCuboidInfo(hitCuboid);
            int idx = 0;
            for (TblColRef dimension : selectedDimensions) {
                int dimIdx = cuboidInfo.getIndexOf(dimension);
                dimIndexes[idx] = dimIdx;
                dimColIdxMap.put(dimension, dimIdx);
                idx++;
            }
            idx = 0;
            for (FunctionDesc metric : selectedMetrics) {
                metricsIndexes[idx] = parsedStreamingCubeInfo.getMetricIndexInAllMetrics(metric);
                idx++;
            }
            this.queryProfile = StreamingQueryProfile.get();
            if (filter != null && aggBufMap != null && !aggBufMap.isEmpty()) {
                byte[] bytes = TupleFilterSerializer.serialize(filter, null, StringCodeSystem.INSTANCE);
                filter = TupleFilterSerializer.deserialize(bytes, StringCodeSystem.INSTANCE);
                Set<TblColRef> unEvaluableColumns = Sets.newHashSet();
                filter = new StreamingBuiltInFunctionTransformer(unEvaluableColumns).transform(filter);
                if (!unEvaluableColumns.isEmpty()) {
                    searchRequest.addNewGroups(unEvaluableColumns);
                }
            }
        }

        @Override
        public Iterator<Record> iterator() {
            if (aggBufMap == null || aggBufMap.isEmpty()) {
                return Collections.emptyIterator();
            }

            return new Iterator<Record>() {
                Entry<String[], MeasureAggregator[]> nextEntry;
                Record oneRecord = new Record(dimIndexes.length, metricsIndexes.length);

                final IEvaluatableTuple oneTuple = new IEvaluatableTuple() {
                    @Override
                    public Object getValue(TblColRef col) {
                        return nextEntry.getKey()[dimColIdxMap.get(col)];
                    }
                };
                @SuppressWarnings("rawtypes")
                final Iterator<Entry<String[], MeasureAggregator[]>> it = aggBufMap.entrySet().iterator();

                @Override
                public boolean hasNext() {
                    boolean result = false;
                    if (nextEntry != null) {
                        result = true;
                    } else {
                        while (it.hasNext()) {
                            nextEntry = it.next();
                            scanCnt++;
                            if (filter != null && !evaluateFilter()) {
                                filterCnt++;
                                continue;
                            }
                            result = true;
                            break;
                        }
                    }
                    if (!result) {
                        nextEntry = null;
                    }
                    return result;
                }

                private boolean evaluateFilter() {
                    return filter.evaluate(oneTuple, StringCodeSystem.INSTANCE);
                }

                @Override
                public Record next() {
                    if (nextEntry == null)
                        throw new NoSuchElementException();
                    
                    try {
                        String[] allDimensions = nextEntry.getKey();
                        MeasureAggregator<?>[] allMetrics = nextEntry.getValue();

                        String[] targetDimensions = new String[dimIndexes.length];
                        MeasureAggregator<?>[] targetMetrics = new MeasureAggregator[metricsIndexes.length];

                        for (int i = 0; i < targetDimensions.length; i++) {
                            targetDimensions[i] = allDimensions[dimIndexes[i]];
                        }

                        for (int i = 0; i < targetMetrics.length; i++) {
                            MeasureAggregator aggregator = allMetrics[metricsIndexes[i]];
                            // need to deep clone the topn aggregator, since topnAggregator.getState()
                            // will change the aggregator state, that will cause some concurrency problem
                            if (aggregator instanceof TopNAggregator) {
                                synchronized (aggregator) {
                                    TopNAggregator topNAggregator = (TopNAggregator) aggregator;
                                    aggregator = topNAggregator.copy();
                                }
                            }
                            targetMetrics[i] = aggregator;
                        }

                        MeasureAggregators aggs = new MeasureAggregators(targetMetrics);
                        Object[] aggrResult = new Object[targetMetrics.length];
                        aggs.collectStates(aggrResult);

                        System.arraycopy(targetDimensions, 0, oneRecord.getDimensions(), 0, targetDimensions.length);
                        System.arraycopy(aggrResult, 0, oneRecord.getMetrics(), 0, aggrResult.length);
                        count++;
                        return oneRecord;
                    } finally {
                        nextEntry = null;
                    }
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public void startRead() {
            if (queryProfile.isDetailProfileEnable()) {
                logger.info("query-{}: start to scan segment-{} memory store",
                        queryProfile.getQueryId(), segmentName);
                String stepName = getQueryStepName();
                queryProfile.startStep(stepName);
            }
        }

        @Override
        public void endRead() {
            queryProfile.incScanRows(scanCnt);
            queryProfile.incFilterRows(filterCnt);
            if (queryProfile.isDetailProfileEnable()) {
                String stepName = getQueryStepName();
                StreamingQueryProfile.ProfileStep profileStep = queryProfile.finishStep(stepName);
                profileStep.stepInfo("scan_count", String.valueOf(scanCnt)).stepInfo("filter_count",
                        String.valueOf(filterCnt));
                logger.info("query-{}: segment-{} memory store scan finished, take {} ms", queryProfile.getQueryId(),
                        segmentName, profileStep.getDuration());
            }
        }

        private String getQueryStepName() {
            return String.format(Locale.ROOT, "segment-%s_mem_store_scan", segmentName);
        }

    }

}

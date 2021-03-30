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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Collections;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;

import org.apache.kylin.shaded.com.google.common.collect.Iterators;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;
import org.apache.kylin.stream.core.query.HavingFilterChecker;
import org.apache.kylin.stream.core.storage.Record;
import org.apache.kylin.stream.core.query.IStreamingSearchResult;
import org.apache.kylin.stream.core.query.ResponseResultSchema;
import org.apache.kylin.stream.core.query.StreamingQueryProfile;
import org.apache.kylin.stream.core.storage.columnar.protocol.CuboidMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.invertindex.IndexSearchResult;
import org.apache.kylin.stream.core.storage.columnar.invertindex.InvertIndexSearcher;
import org.apache.kylin.stream.core.util.StreamFilterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FragmentSearchResult implements IStreamingSearchResult {
    private static Logger logger = LoggerFactory.getLogger(FragmentSearchResult.class);
    private TupleFilter filter;
    private InvertIndexSearcher iiSearcher;
    private DataSegmentFragment fragment;
    private ResponseResultSchema responseSchema;
    private ColumnarRecordCodec recordCodec;

    private Set<TblColRef> groups;
    private TupleFilter havingFilter;

    private FragmentCuboidReader fragmentCuboidReader;

    private StreamingQueryProfile queryProfile;
    private int filterRowCnt = 0;
    private int finalRowCnt = 0;

    /**
     *
     * @param fragment
     * @param fragmentData
     * @param cuboidMetaInfo
     * @param filter
     * @param recordCodec
     */
    public FragmentSearchResult(final DataSegmentFragment fragment, final FragmentData fragmentData,
            final CuboidMetaInfo cuboidMetaInfo, ResponseResultSchema responseSchema,
            final TupleFilter filter, final Set<TblColRef> groups, final TupleFilter havingFilter, ColumnarRecordCodec recordCodec) throws IOException {
        this.fragment = fragment;
        this.filter = filter;
        this.responseSchema = responseSchema;
        this.groups = groups;
        this.havingFilter = havingFilter;

        TblColRef[] dimensions = responseSchema.getDimensions();

        ByteBuffer readBuffer = fragmentData.getDataReadBuffer();
        this.iiSearcher = new InvertIndexSearcher(cuboidMetaInfo, dimensions, readBuffer.asReadOnlyBuffer());

        CubeDesc cubeDesc = responseSchema.getCubeDesc();

        this.recordCodec = recordCodec;
        this.fragmentCuboidReader = new FragmentCuboidReader(cubeDesc, fragmentData, cuboidMetaInfo, dimensions,
                responseSchema.getMeasureDescs(), recordCodec.getDimensionEncodings());
        this.queryProfile = StreamingQueryProfile.get();
    }

    public Iterator<Record> iterator() {
        final Iterator<RawRecord> sourceRecords = searchFragment();
        FilteredAndAggregatedRecords filterAggrRecords = new FilteredAndAggregatedRecords(sourceRecords,
                responseSchema, recordCodec, filter, groups, havingFilter);
        return filterAggrRecords.iterator();
    }

    private Iterator<RawRecord> searchFragment() {
        IndexSearchResult indexSearchResult = searchFromIndex();
        Iterator<RawRecord> result;
        // Full table scan
        if (indexSearchResult == null || indexSearchResult.needFullScan()) {
            result = fragmentCuboidReader.iterator();
            queryProfile.addStepInfo(getFragmentDataScanStep(), "use_index", "false");
        } else {
            queryProfile.addStepInfo(getFragmentDataScanStep(), "use_index", "true");
            if (indexSearchResult.rows == null) {
                if (queryProfile.isDetailProfileEnable()) {
                    logger.info("query-{}: no data match the query in the file segment-{}_fragment-{}",
                            queryProfile.getQueryId(), fragment.getSegmentName(), fragment.getFragmentId());
                }
                return Collections.emptyIterator();
            }
            final Iterator<Integer> rows = indexSearchResult.rows;
            result = new Iterator<RawRecord>() {
                @Override
                public boolean hasNext() {
                    return rows.hasNext();
                }

                @Override
                public RawRecord next() {
                    return fragmentCuboidReader.read(rows.next() - 1);
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
        return result;
    }

    private IndexSearchResult searchFromIndex() {
        if (queryProfile.isDetailProfileEnable()) {
            queryProfile.startStep(getFragmentIdxSearchStep());
        }
        IndexSearchResult result = iiSearcher.search(filter);
        if (queryProfile.isDetailProfileEnable()) {
            queryProfile.finishStep(getFragmentIdxSearchStep());
        }
        return result;
    }

    private String getFragmentIdxSearchStep() {
        return String.format(Locale.ROOT, "segment-%s_fragment-%s_idx_search", fragment.getSegmentName(), fragment.getFragmentId());
    }

    private String getFragmentDataScanStep() {
        return String.format(Locale.ROOT, "segment-%s_fragment-%s_data_scan", fragment.getSegmentName(), fragment.getFragmentId());
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public void startRead() {
        if (queryProfile.isDetailProfileEnable()) {
            String stepName = getFragmentDataScanStep();
            queryProfile.startStep(stepName);
            logger.info("query-{}: start to search segment-{}_fragment-{} file",
                    queryProfile.getQueryId(), fragment.getSegmentName(), fragment.getFragmentId());
        }
    }

    @Override
    public void endRead() {
        long scanRowCnt = fragmentCuboidReader.getReadRowCount();
        queryProfile.incScanRows(scanRowCnt);
        queryProfile.incFilterRows(filterRowCnt);
        if (queryProfile.isDetailProfileEnable()) {
            String stepName = getFragmentDataScanStep();
            StreamingQueryProfile.ProfileStep profileStep = queryProfile.finishStep(stepName).stepInfo(
                    "row_count", String.valueOf(fragmentCuboidReader.getReadRowCount()));
            logger.info("query-{}: segment-{}_fragment-{} scan finished, scan {} rows, filter {} rows, return {} rows, take {} ms",
                    queryProfile.getQueryId(), fragment.getSegmentName(), fragment.getFragmentId(), scanRowCnt, filterRowCnt, finalRowCnt,
                    profileStep.getDuration());
        }
    }

    public class FilteredAndAggregatedRecords implements Iterable<Record> {
        private TupleFilter filter;
        private TupleFilter havingFilter;
        private Iterator<RawRecord> sourceRecords;
        private AggregationCache aggrCache;
        private ResponseResultSchema schema;
        private int[] groupIndexes;

        private ColumnarRecordCodec recordDecoder;
        private int pushDownLimit = Integer.MAX_VALUE;

        RawRecord next;
        final IEvaluatableTuple oneTuple = new IEvaluatableTuple() {
            @Override
            public Object getValue(TblColRef col) {
                return new ByteArray(next.getDimensions()[schema.getIndexOfDimension(col)]);
            }
        };
        final IFilterCodeSystem<ByteArray> filterCodeSystem = StreamFilterUtil.getStreamingFilterCodeSystem();

        public FilteredAndAggregatedRecords(Iterator<RawRecord> sourceRecords, ResponseResultSchema schema, ColumnarRecordCodec recordDecoder,
                                            TupleFilter filter, Set<TblColRef> groups, TupleFilter havingFilter) {
            this.sourceRecords = sourceRecords;
            this.schema = schema;
            this.recordDecoder = recordDecoder;
            this.filter = filter;
            this.havingFilter = havingFilter;
            this.groupIndexes = new int[groups.size()];
            int i = 0;
            for (TblColRef group : groups) {
                groupIndexes[i] = schema.getIndexOfDimension(group);
                i++;
            }
            if (groupIndexes.length == 0) {
                this.aggrCache = new OneValueAggregationCache();
            } else {
                this.aggrCache = new TreeMapAggregationCache();
            }
        }

        @Override
        public Iterator<Record> iterator() {
            if (hasAggregation()) {
                while (sourceRecords.hasNext()) {
                    RawRecord rawRecord = sourceRecords.next();
                    if (filter != null && !satisfyFilter(rawRecord)) {
                        filterRowCnt ++;
                    } else {
                        aggrCache.aggregate(rawRecord);
                    }
                }
                return aggrCache.iterator();
            } else {
                return transformAndFilterRecords();
            }
        }

        private Iterator<Record> transformAndFilterRecords() {
            return new Iterator<Record>() {
                Record oneRecord = new Record(schema.getDimensionCount(), schema.getMetricsCount());
                @Override
                public boolean hasNext() {
                    if (next != null)
                        return true;

                    while (sourceRecords.hasNext()) {
                        next = sourceRecords.next();
                        if (filter != null && !evaluateFilter()) {
                            filterRowCnt ++;
                            continue;
                        }
                        return true;
                    }
                    next = null;
                    return false;
                }

                private boolean evaluateFilter() {
                    return filter.evaluate(oneTuple, filterCodeSystem);
                }

                @Override
                public Record next() {
                    // fetch next record
                    if (next == null) {
                        hasNext();
                        if (next == null)
                            throw new NoSuchElementException();
                    }
                    byte[][] rawDimVals = next.getDimensions();
                    for (int i = 0; i < rawDimVals.length; i++) {
                        oneRecord.setDimension(i, recordDecoder.decodeDimension(i, rawDimVals[i]));
                    }
                    // no metrics here, will go to aggregate result if there are metrics.
                    next = null;
                    return oneRecord;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }

            };
        }

        private boolean hasAggregation() {
            return groupIndexes.length > 0 || schema.getMetricsCount() > 0;
        }

        private boolean satisfyFilter(RawRecord rawRecord) {
            next = rawRecord;
            return filter.evaluate(oneTuple, filterCodeSystem);
        }

        private MeasureAggregator[] newAggregators() {
            String[] aggrFuncs = schema.getAggrFuncs();
            MeasureAggregator<?>[] result = new MeasureAggregator[aggrFuncs.length];
            for (int i = 0; i < result.length; i++) {
                result[i] = MeasureAggregator.create(aggrFuncs[i], schema.getMetricsDataType(i));
            }
            return result;
        }

        class TreeMapAggregationCache implements AggregationCache {

            final Comparator<byte[][]> bytesComparator = new Comparator<byte[][]>() {
                @Override
                public int compare(byte[][] o1, byte[][] o2) {
                    for (int i = 0; i < groupIndexes.length; i++) {
                        int groupIdx = groupIndexes[i];
                        int result = Bytes.compareTo(o1[groupIdx], o2[groupIdx]);
                        if (result != 0) {
                            return result;
                        }
                    }
                    return 0;
                }
            };

            SortedMap<byte[][], MeasureAggregator[]> aggBufMap;

            public TreeMapAggregationCache() {
                aggBufMap = createBuffMap();
            }

            private SortedMap<byte[][], MeasureAggregator[]> createBuffMap() {
                return Maps.newTreeMap(bytesComparator);
            }

            public boolean aggregate(RawRecord r) {
                byte[][] dimVals = r.getDimensions();
                byte[][] metricsVals = r.getMetrics();
                MeasureAggregator[] aggrs = aggBufMap.get(dimVals);
                if (aggrs == null) {
                    //for storage push down limit
                    if (aggBufMap.size() >= pushDownLimit) {
                        return false;
                    }
                    byte[][] copyDimVals = new byte[schema.getDimensionCount()][];

                    for(int i=0;i<dimVals.length;i++){
                        copyDimVals[i] = new byte[dimVals[i].length];
                        System.arraycopy(dimVals[i], 0, copyDimVals[i], 0, dimVals[i].length);
                    }

                    aggrs = newAggregators();
                    aggBufMap.put(copyDimVals, aggrs);
                }
                for (int i = 0; i < aggrs.length; i++) {
                    Object metrics = recordDecoder.decodeMetrics(i, metricsVals[i]);
                    aggrs[i].aggregate(metrics);
                }
                return true;
            }

            @Override
            public void close() throws RuntimeException {

            }

            public Iterator<Record> iterator() {
                Iterator<Entry<byte[][], MeasureAggregator[]>> it = aggBufMap.entrySet().iterator();

                final Iterator<Entry<byte[][], MeasureAggregator[]>> input = it;

                return new Iterator<Record>() {

                    final Record oneRecord = new Record(schema.getDimensionCount(), schema.getMetricsCount());
                    Entry<byte[][], MeasureAggregator[]> returningEntry = null;
                    final HavingFilterChecker havingFilterChecker = (havingFilter == null) ? null
                            : new HavingFilterChecker(havingFilter, schema);

                    @Override
                    public boolean hasNext() {
                        while (returningEntry == null && input.hasNext()) {
                            returningEntry = input.next();
                            if (havingFilterChecker != null) {
                                if (!havingFilterChecker.check(returningEntry.getValue())) {
                                    returningEntry = null;
                                }
                            }
                        }
                        return returningEntry != null;
                    }

                    @Override
                    public Record next() {
                        byte[][] rawDimVals = returningEntry.getKey();
                        for (int i = 0; i < rawDimVals.length; i++) {
                            oneRecord.setDimension(i, recordDecoder.decodeDimension(i, rawDimVals[i]));
                        }
                        MeasureAggregator[] measures = returningEntry.getValue();
                        for (int i = 0; i < measures.length; i++) {
                            oneRecord.setMetric(i, measures[i].getState());
                        }
                        finalRowCnt ++;
                        returningEntry = null;
                        return oneRecord;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

        }

        class OneValueAggregationCache implements AggregationCache {
            MeasureAggregator[] aggrs;
            byte[][] rawDimValues;

            public OneValueAggregationCache() {
                aggrs = newAggregators();
            }

            public boolean aggregate(RawRecord r) {
                if (rawDimValues == null) {
                    byte[][] tmpDimValues = r.getDimensions();
                    rawDimValues = new byte[tmpDimValues.length][];
                    for (int i = 0; i < tmpDimValues.length; i++) {
                        rawDimValues[i] = new byte[tmpDimValues[i].length];
                        System.arraycopy(tmpDimValues[i], 0, rawDimValues[i], 0, tmpDimValues[i].length);
                    }
                }
                byte[][] metricsVals = r.getMetrics();
                for (int i = 0; i < aggrs.length; i++) {
                    Object metrics = recordDecoder.decodeMetrics(i, metricsVals[i]);
                    aggrs[i].aggregate(metrics);
                }
                return true;
            }

            @Override
            public void close() throws RuntimeException {
            }

            public Iterator<Record> iterator() {
                if (rawDimValues == null) {
                    return Collections.emptyIterator();
                }
                HavingFilterChecker havingFilterChecker = (havingFilter == null) ? null
                        : new HavingFilterChecker(havingFilter, schema);
                if (havingFilterChecker == null) {
                    return Iterators.singletonIterator(createRecord());
                }
                if (havingFilterChecker.check(aggrs)) {
                    return Iterators.singletonIterator(createRecord());
                } else {
                    return Collections.emptyIterator();
                }
            }

            Record createRecord() {
                Record record = new Record(schema.getDimensionCount(), schema.getMetricsCount());
                for (int i = 0; i < rawDimValues.length; i++) {
                    record.setDimension(i, recordDecoder.decodeDimension(i, rawDimValues[i]));
                }
                for (int i = 0; i < aggrs.length; i++) {
                    record.setMetric(i, aggrs[i].getState());
                }
                finalRowCnt ++;
                return record;
            }

        }

    }
    interface AggregationCache extends Closeable {
        boolean aggregate(RawRecord r);
        Iterator<Record> iterator();
    }
}

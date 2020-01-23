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

package org.apache.kylin.stream.core.query;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.gridtable.StorageSideBehavior;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.stream.core.storage.StreamingCubeSegment;
import org.apache.kylin.stream.core.storage.StreamingSegmentManager;
import org.apache.kylin.stream.core.storage.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingCubeDataSearcher {
    private static Logger logger = LoggerFactory.getLogger(StreamingCubeDataSearcher.class);

    private static int TIMEOUT = Integer.MAX_VALUE;

    private StreamingSegmentManager streamingSegmentManager;
    private String cubeName;
    private CubeDesc cubeDesc;

    public StreamingCubeDataSearcher(StreamingSegmentManager streamingSegmentManager) {
        this.streamingSegmentManager = streamingSegmentManager;
        this.cubeName = streamingSegmentManager.getCubeInstance().getName();
        CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
        this.cubeDesc = cubeInstance.getDescriptor();
    }

    public ITupleIterator search(TupleInfo returnTupleInfo, TupleFilter filter, TupleFilter havingFilter,
            Set<TblColRef> dimensions, Set<TblColRef> groups, Set<FunctionDesc> metrics, boolean allowStorageAggregation) {
        StreamingSearchContext searchRequest = new StreamingSearchContext(cubeDesc, dimensions, groups, metrics,
                filter, havingFilter);
        IStreamingSearchResult searchResult = doSearch(searchRequest, -1, allowStorageAggregation);
        StreamingTupleConverter tupleConverter = new StreamingTupleConverter(searchRequest.getRespResultSchema(),
                returnTupleInfo);
        return transformToTupleIterator(tupleConverter, searchResult, returnTupleInfo);
    }

    public IStreamingSearchResult doSearch(StreamingSearchContext searchRequest, long minSegmentTime,
                                           boolean allowStorageAggregation) {
        StreamingQueryProfile queryProfile = StreamingQueryProfile.get();
        try {
            logger.info("query-{}: use cuboid {} to serve the query", queryProfile.getQueryId(),
                    searchRequest.getHitCuboid());
            ResultCollector resultCollector = getResultCollector();
            Collection<StreamingCubeSegment> segments = streamingSegmentManager.getAllSegments();
            StreamingDataQueryPlanner scanRangePlanner = searchRequest.getQueryPlanner();
            for (StreamingCubeSegment queryableSegment : segments) {
                if (!queryableSegment.isLongLatencySegment() && queryableSegment.getDateRangeStart() < minSegmentTime) {
                    String segmentName = queryableSegment.getSegmentName();
                    queryProfile.skipSegment(segmentName);
                    logger.info("query-{}: skip segment {}, it is smaller than the min segment time:{}",
                            queryProfile.getQueryId(), segmentName, minSegmentTime);
                    continue;
                }

                if (scanRangePlanner.canSkip(queryableSegment.getDateRangeStart(), queryableSegment.getDateRangeEnd())) {
                    String segmentName = queryableSegment.getSegmentName();
                    queryProfile.skipSegment(segmentName);
                    logger.info("query-{}: skip segment {}", queryProfile.getQueryId(),
                            queryableSegment.getSegmentName());
                } else {
                    String segmentName = queryableSegment.getSegmentName();
                    queryProfile.includeSegment(segmentName);
                    logger.info("query-{}: include segment {}", queryProfile.getQueryId(), segmentName);

                    queryableSegment.getSegmentStore().search(searchRequest, resultCollector);
                }
            }

            return createFinalResult(resultCollector, searchRequest, allowStorageAggregation, queryProfile);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ResultCollector getResultCollector() {
        int useThreads = cubeDesc.getConfig().getStreamingReceiverUseThreadsPerQuery();
        if (useThreads > 1) {
            return new MultiThreadsResultCollector(useThreads, TIMEOUT);
        } else {
            return new SingleThreadResultCollector();
        }
    }

    private IStreamingSearchResult createFinalResult(final ResultCollector resultCollector,
                                                     final StreamingSearchContext searchRequest, boolean allowStorageAggregation,
                                                     StreamingQueryProfile queryProfile) throws IOException {
        IStreamingSearchResult finalResult = resultCollector;

        if (queryProfile.getStorageBehavior().ordinal() <= StorageSideBehavior.SCAN.ordinal()) {
            return finalResult;
        }
        if (allowStorageAggregation) {
            finalResult = new StreamAggregateSearchResult(finalResult, searchRequest);
        }
        return finalResult;
    }

    private ITupleIterator transformToTupleIterator(final StreamingTupleConverter tupleConverter,
                                                    final IStreamingSearchResult searchResult, final TupleInfo returnTupleInfo) {
        final Tuple tuple = new Tuple(returnTupleInfo);

        final Iterator<Record> recordIterator = searchResult.iterator();
        return new ITupleIterator() {
            @Override
            public void close() {
                try {
                    searchResult.close();
                } catch (IOException e) {
                    logger.warn("exception when close gtscanner", e);
                }
            }

            @Override
            public boolean hasNext() {
                return recordIterator.hasNext();
            }

            @Override
            public ITuple next() {
                tupleConverter.translateResult(recordIterator.next(), tuple);
                return tuple;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("not support");
            }
        };
    }

    public class StreamAggregateSearchResult implements IStreamingSearchResult {
        private IStreamingSearchResult inputSearchResult;
        private RecordsAggregator recordsAggregator;

        public StreamAggregateSearchResult(IStreamingSearchResult inputSearchResult,
                StreamingSearchContext searchRequest) {
            this.inputSearchResult = inputSearchResult;
            this.recordsAggregator = new RecordsAggregator(searchRequest.getRespResultSchema(), searchRequest.getAllGroups(), searchRequest.getHavingFilter());
        }

        @Override
        public void startRead() {

        }

        @Override
        public void endRead() {

        }

        @Override
        public void close() throws IOException {
            inputSearchResult.close();
        }

        @Override
        public Iterator<Record> iterator() {
            recordsAggregator.aggregate(inputSearchResult.iterator());
            return recordsAggregator.iterator();
        }
    }

}

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

package org.apache.kylin.storage.stream;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.CompoundTupleIterator;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.gtrecord.CubeSegmentScanner;
import org.apache.kylin.storage.gtrecord.GTCubeStorageQueryRequest;
import org.apache.kylin.storage.gtrecord.SequentialCubeTupleIterator;
import org.apache.kylin.storage.hbase.cube.v2.CubeStorageQuery;
import org.apache.kylin.storage.stream.rpc.IStreamDataSearchClient;
import org.apache.kylin.stream.core.query.StreamingDataQueryPlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

/**
 * Streaming storage query
 */
public class StreamStorageQuery extends CubeStorageQuery {

    private static final Logger logger = LoggerFactory.getLogger(StreamStorageQuery.class);

    private final IStreamDataSearchClient realTimeSearchClient;

    public StreamStorageQuery(CubeInstance cube, IStreamDataSearchClient realTimeSearchClient) {
        super(cube);
        this.realTimeSearchClient = realTimeSearchClient;
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo) {
        GTCubeStorageQueryRequest request = getStorageQueryRequest(context, sqlDigest, returnTupleInfo);

        List<CubeSegmentScanner> scanners = Lists.newArrayList();
        long maxHistorySegmentTime = -1;
        StreamingDataQueryPlanner segmentsPlanner = new StreamingDataQueryPlanner(cubeInstance.getDescriptor(),
                request.getFilter());
        for (CubeSegment cubeSeg : cubeInstance.getSegments(SegmentStatusEnum.READY)) {
            TSRange segmentRange = cubeSeg.getTSRange();
            if (segmentRange.end.v > maxHistorySegmentTime) {
                maxHistorySegmentTime = cubeSeg.getTSRange().end.v;
            }
            CubeSegmentScanner scanner;

            if (cubeDesc.getConfig().isSkippingEmptySegments() && cubeSeg.getInputRecords() == 0) {
                logger.info("Skip cube segment {} because its input record is 0", cubeSeg);
                continue;
            }

            if (segmentsPlanner.canSkip(segmentRange.start.v, segmentRange.end.v)) {
                logger.info("Skip cube segment {} because of not satisfy filter:{}", cubeSeg, request.getFilter());
                continue;
            }

            scanner = new CubeSegmentScanner(cubeSeg, request.getCuboid(), request.getDimensions(),
                    request.getGroups(), request.getDynGroups(), request.getDynGroupExprs(), request.getMetrics(),
                    request.getDynFuncs(), request.getFilter(), request.getHavingFilter(), request.getContext());
            if (!scanner.isSegmentSkipped())
                scanners.add(scanner);
        }

        ITupleIterator historyResult;
        if (scanners.isEmpty()) {
            historyResult = ITupleIterator.EMPTY_TUPLE_ITERATOR;
        } else {
            historyResult = new SequentialCubeTupleIterator(scanners, request.getCuboid(), request.getDimensions(),
                    request.getDynGroups(), request.getGroups(), request.getMetrics(), returnTupleInfo, context, sqlDigest);
        }
        Set<TblColRef> dimensionsD = request.getDimensions();
        if (dimensionsD.isEmpty()) {
            dimensionsD = Sets.newHashSet(request.getCuboid().getColumns()); // temporary fix for query like: select count(1) from TABLE
        }

        ITupleIterator realTimeResult;
        if (segmentsPlanner.canSkip(maxHistorySegmentTime, Long.MAX_VALUE)) {
            logger.info("Skip scan realTime data, {}", maxHistorySegmentTime);
            realTimeResult = ITupleIterator.EMPTY_TUPLE_ITERATOR;
        } else {
            boolean isSelectAllQuery = isSelectAllQuery(request.getCuboid(), request.getGroups(), request.getFilter());
            int limitPushDown = isSelectAllQuery ? context.getFinalPushDownLimit() : Integer.MAX_VALUE;
            realTimeResult = realTimeSearchClient.search(maxHistorySegmentTime, cubeInstance, returnTupleInfo,
                    request.getFilter(), dimensionsD, request.getGroups(), request.getMetrics(), limitPushDown,
                    !isSelectAllQuery);
        }
        return new CompoundTupleIterator(Arrays.asList(historyResult, realTimeResult));
    }

    // only 'select *' query don't need real time storage aggregation, and push down limit
    private boolean isSelectAllQuery(Cuboid cuboid, Set<TblColRef> groupsD, TupleFilter filterD) {
        if (Cuboid.getBaseCuboidId(cubeDesc) == cuboid.getId() && filterD == null
                && cuboid.getColumns().size() == groupsD.size()) {
            return true;
        }
        return false;
    }

}

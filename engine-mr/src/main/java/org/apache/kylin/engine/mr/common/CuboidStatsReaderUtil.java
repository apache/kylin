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

package org.apache.kylin.engine.mr.common;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class CuboidStatsReaderUtil {

    private static final Logger logger = LoggerFactory.getLogger(CuboidStatsReaderUtil.class);

    public static Map<Long, Long> readCuboidStatsFromCube(Set<Long> cuboidIds, CubeInstance cubeInstance) {
        Map<Long, Long> statisticsMerged = null;
        try {
            statisticsMerged = readCuboidStatsAndSizeFromCube(cuboidIds, cubeInstance).getFirst();
        } catch (IOException e) {
            logger.warn("Fail to read statistics for cube " + cubeInstance.getName() + " due to " + e);
        }
        return statisticsMerged == null ? Collections.emptyMap() : statisticsMerged;
    }

    public static Pair<Map<Long, Long>, Map<Long, Double>> readCuboidStatsAndSizeFromCube(Set<Long> cuboidIds,
            CubeInstance cube) throws IOException {
        Preconditions.checkNotNull(cuboidIds, "The cuboid set can not be null");
        Preconditions.checkNotNull(cube, "The cube instance can not be null");

        List<CubeSegment> segmentList = cube.getSegments(SegmentStatusEnum.READY);
        Map<Long, Long> statisticsMerged = Maps.newHashMapWithExpectedSize(cuboidIds.size());
        Map<Long, Double> sizeMerged = Maps.newHashMapWithExpectedSize(cuboidIds.size());
        readCuboidStatsFromSegments(cuboidIds, segmentList, statisticsMerged, sizeMerged);
        return new Pair<>(statisticsMerged, sizeMerged);
    }

    public static Map<Long, Long> readCuboidStatsFromSegments(Set<Long> cuboidIds, List<CubeSegment> segmentList)
            throws IOException {
        Map<Long, Long> statisticsMerged = Maps.newHashMapWithExpectedSize(cuboidIds.size());
        readCuboidStatsFromSegments(cuboidIds, segmentList, statisticsMerged,
                Maps.<Long, Double> newHashMapWithExpectedSize(cuboidIds.size()));
        return statisticsMerged.isEmpty() ? null : statisticsMerged;
    }

    public static Map<Long, Double> readCuboidSizeFromCube(Map<Long, Long> statistics, CubeInstance cube)
            throws IOException {
        List<CubeSegment> segmentList = cube.getSegments(SegmentStatusEnum.READY);
        Map<Long, Double> sizeMerged = Maps.newHashMapWithExpectedSize(statistics.size());
        for (CubeSegment pSegment : segmentList) {
            CubeStatsReader pReader = new CubeStatsReader(pSegment, null, pSegment.getConfig());
            Map<Long, Double> pSizeMap = CubeStatsReader.getCuboidSizeMapFromRowCount(pSegment, statistics,
                    pReader.sourceRowCount);
            for (Long pCuboid : statistics.keySet()) {
                Double pSize = sizeMerged.get(pCuboid);
                sizeMerged.put(pCuboid, pSize == null ? pSizeMap.get(pCuboid) : pSize + pSizeMap.get(pCuboid));
            }
        }
        int nSegment = segmentList.size();
        if (nSegment <= 1) {
            return sizeMerged;
        }
        for (Long pCuboid : statistics.keySet()) {
            sizeMerged.put(pCuboid, sizeMerged.get(pCuboid) / nSegment);
        }
        return sizeMerged;
    }

    private static void readCuboidStatsFromSegments(Set<Long> cuboidSet, List<CubeSegment> segmentList,
            final Map<Long, Long> statisticsMerged, final Map<Long, Double> sizeMerged) throws IOException {
        if (segmentList == null || segmentList.isEmpty()) {
            return;
        }
        int nSegment = segmentList.size();

        Map<Long, HLLCounter> cuboidHLLMapMerged = Maps.newHashMapWithExpectedSize(cuboidSet.size());
        Map<Long, Double> sizeMapMerged = Maps.newHashMapWithExpectedSize(cuboidSet.size());
        for (CubeSegment pSegment : segmentList) {
            CubeStatsReader pReader = new CubeStatsReader(pSegment, null, pSegment.getConfig());
            Map<Long, HLLCounter> pHLLMap = pReader.getCuboidRowHLLCounters();
            if (pHLLMap == null || pHLLMap.isEmpty()) {
                logger.info("Cuboid Statistics for segment " + pSegment.getName() + " is not enabled.");
                nSegment--;
                continue;
            }
            Map<Long, Double> pSizeMap = pReader.getCuboidSizeMap();
            for (Long pCuboid : cuboidSet) {
                HLLCounter pInnerHLL = pHLLMap.get(pCuboid);
                Preconditions.checkNotNull(pInnerHLL, "statistics should exist for cuboid " + pCuboid + " of segment "
                        + pSegment.getCubeDesc().getName() + "[" + pSegment.getName() + "]");
                if (cuboidHLLMapMerged.get(pCuboid) != null) {
                    cuboidHLLMapMerged.get(pCuboid).merge(pInnerHLL);
                } else {
                    cuboidHLLMapMerged.put(pCuboid, pInnerHLL);
                }

                Double pSize = sizeMapMerged.get(pCuboid);
                sizeMapMerged.put(pCuboid, pSize == null ? pSizeMap.get(pCuboid) : pSizeMap.get(pCuboid) + pSize);
            }
        }

        if (nSegment < 1) {
            return;
        }
        for (Long pCuboid : cuboidSet) {
            statisticsMerged.put(pCuboid, cuboidHLLMapMerged.get(pCuboid).getCountEstimate());
            sizeMerged.put(pCuboid, sizeMapMerged.get(pCuboid));
        }
    }

    public static Map<Long, Long> readCuboidStatsFromSegment(Set<Long> cuboidIds, CubeSegment cubeSegment)
            throws IOException {
        Pair<Map<Long, Long>, Long> stats = readCuboidStatsWithSourceFromSegment(cuboidIds, cubeSegment);
        return stats == null ? null : stats.getFirst();
    }

    public static Pair<Map<Long, Long>, Long> readCuboidStatsWithSourceFromSegment(Set<Long> cuboidIds,
            CubeSegment cubeSegment) throws IOException {
        if (cubeSegment == null) {
            logger.warn("The cube segment can not be " + null);
            return null;
        }

        CubeStatsReader cubeStatsReader = new CubeStatsReader(cubeSegment, null, cubeSegment.getConfig());
        if (cubeStatsReader.getCuboidRowEstimatesHLL() == null
                || cubeStatsReader.getCuboidRowEstimatesHLL().isEmpty()) {
            logger.info("Cuboid Statistics is not enabled.");
            return null;
        }

        Map<Long, Long> cuboidsWithStatsAll = cubeStatsReader.getCuboidRowEstimatesHLL();
        Map<Long, Long> cuboidsWithStats = Maps.newHashMapWithExpectedSize(cuboidIds.size());
        for (Long cuboid : cuboidIds) {
            Long rowEstimate = cuboidsWithStatsAll.get(cuboid);
            if (rowEstimate == null) {
                logger.warn("Cannot get the row count stats for cuboid " + cuboid);
            } else {
                cuboidsWithStats.put(cuboid, rowEstimate);
            }
        }
        return new Pair<>(cuboidsWithStats, cubeStatsReader.sourceRowCount);
    }
}

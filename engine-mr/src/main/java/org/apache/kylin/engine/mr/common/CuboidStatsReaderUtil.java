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

    public static Map<Long, Long> readCuboidStatsFromCube(Set<Long> cuboidIds, CubeInstance cubeInstance)
            throws IOException {
        Map<Long, Long> statisticsMerged = readCuboidStatsAndSizeFromCube(cuboidIds, cubeInstance).getFirst();
        return statisticsMerged.isEmpty() ? null : statisticsMerged;
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

    private static void readCuboidStatsFromSegments(Set<Long> cuboidSet, List<CubeSegment> segmentList,
            final Map<Long, Long> statisticsMerged, final Map<Long, Double> sizeMerged) throws IOException {
        if (segmentList == null || segmentList.isEmpty()) {
            return;
        }
        int nSegment = segmentList.size();

        Map<Long, HLLCounter> cuboidHLLMapMerged = Maps.newHashMapWithExpectedSize(cuboidSet.size());
        Map<Long, Double> sizeMapMerged = Maps.newHashMapWithExpectedSize(cuboidSet.size());
        for (CubeSegment pSegment : segmentList) {
            CubeStatsReader pReader = new CubeStatsReader(pSegment, pSegment.getConfig());
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
        if (cubeSegment == null) {
            logger.warn("The cube segment can not be " + null);
            return null;
        }

        CubeStatsReader cubeStatsReader = new CubeStatsReader(cubeSegment, cubeSegment.getConfig());
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
        return cuboidsWithStats;
    }
}

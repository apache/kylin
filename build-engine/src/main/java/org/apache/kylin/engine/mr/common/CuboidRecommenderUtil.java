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
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.cuboid.algorithm.CuboidRecommender;
import org.apache.kylin.cube.cuboid.algorithm.CuboidStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuboidRecommenderUtil {

    private static final Logger logger = LoggerFactory.getLogger(CuboidRecommenderUtil.class);
    private static final String BASE_CUBOID_COUNT_IN_CUBOID_STATISTICS_IS_ZERO = "Base cuboid count in cuboid statistics is 0.";

    /** Trigger cube planner phase one */
    public static Map<Long, Long> getRecommendCuboidList(CubeSegment segment) throws IOException {
        if (segment == null) {
            return null;
        }

        CubeStatsReader cubeStatsReader = new CubeStatsReader(segment, null, segment.getConfig());
        if (cubeStatsReader.getCuboidRowEstimatesHLL() == null
                || cubeStatsReader.getCuboidRowEstimatesHLL().isEmpty()) {
            logger.info("Cuboid Statistics is not enabled.");
            return null;
        }
        CubeInstance cube = segment.getCubeInstance();
        long baseCuboid = cube.getCuboidScheduler().getBaseCuboidId();
        if ((cubeStatsReader.getCuboidRowEstimatesHLL().get(baseCuboid) == null
                || cubeStatsReader.getCuboidRowEstimatesHLL().get(baseCuboid) == 0L)
                && segment.getConfig().isBuildBaseCuboid()) {
            logger.info(BASE_CUBOID_COUNT_IN_CUBOID_STATISTICS_IS_ZERO);
            return null;
        }

        Set<Long> mandatoryCuboids = segment.getCubeDesc().getMandatoryCuboids();

        String key = cube.getName();
        CuboidStats cuboidStats = new CuboidStats.Builder(key, baseCuboid, cubeStatsReader.getCuboidRowEstimatesHLL(), cubeStatsReader.getCuboidSizeMap())
                .setMandatoryCuboids(mandatoryCuboids)
                .setBPUSMinBenefitRatio(segment.getConfig().getCubePlannerBPUSMinBenefitRatio())
                .build();
        return CuboidRecommender.getInstance().getRecommendCuboidList(cuboidStats, segment.getConfig(),
                !mandatoryCuboids.isEmpty());
    }

    /** Trigger cube planner phase two for optimization */
    public static Map<Long, Long> getRecommendCuboidList(CubeInstance cube, Map<Long, Long> hitFrequencyMap,
            Map<Long, Map<Long, Pair<Long, Long>>> rollingUpCountSourceMap) throws IOException {

        CuboidScheduler cuboidScheduler = cube.getCuboidScheduler();
        Set<Long> currentCuboids = cuboidScheduler.getAllCuboidIds();
        Pair<Map<Long, Long>, Map<Long, Double>> statsPair = CuboidStatsReaderUtil
                .readCuboidStatsAndSizeFromCube(currentCuboids, cube);
        long baseCuboid = cuboidScheduler.getBaseCuboidId();
        if ((statsPair.getFirst().get(baseCuboid) == null || statsPair.getFirst().get(baseCuboid) == 0L)
                && cube.getConfig().isBuildBaseCuboid()) {
            logger.info(BASE_CUBOID_COUNT_IN_CUBOID_STATISTICS_IS_ZERO);
            return null;
        }

        KylinConfig config = cube.getConfig();
        String key = cube.getName();
        double queryUncertaintyRatio = config.getCubePlannerQueryUncertaintyRatio();
        double bpusMinBenefitRatio = config.getCubePlannerBPUSMinBenefitRatio();
        CuboidStats cuboidStats = new CuboidStats.Builder(key, baseCuboid, statsPair.getFirst(),
                statsPair.getSecond()) {
            @Override
            public Map<Long, Double> estimateCuboidsSize(Map<Long, Long> statistics) {
                try {
                    return CuboidStatsReaderUtil.readCuboidSizeFromCube(statistics, cube);
                } catch (IOException e) {
                    logger.warn("Fail to get cuboid size from cube due to ", e);
                    return null;
                }
            }
        }.setQueryUncertaintyRatio(queryUncertaintyRatio) //
                .setBPUSMinBenefitRatio(bpusMinBenefitRatio) //
                .setHitFrequencyMap(hitFrequencyMap) //
                .setRollingUpCountSourceMap(rollingUpCountSourceMap) //
                .build();
        return CuboidRecommender.getInstance().getRecommendCuboidList(cuboidStats, config);
    }

    /** For future segment level recommend */
    public static Map<Long, Long> getRecommendCuboidList(CubeSegment segment, Map<Long, Long> hitFrequencyMap,
            Map<Long, Map<Long, Pair<Long, Long>>> rollingUpCountSourceMap, boolean ifForceRecommend)
            throws IOException {
        if (segment == null) {
            return null;
        }

        CubeStatsReader cubeStatsReader = new CubeStatsReader(segment, null, segment.getConfig());
        if (cubeStatsReader.getCuboidRowEstimatesHLL() == null
                || cubeStatsReader.getCuboidRowEstimatesHLL().isEmpty()) {
            logger.info("Cuboid Statistics is not enabled.");
            return null;
        }
        CubeInstance cube = segment.getCubeInstance();
        long baseCuboid = cube.getCuboidScheduler().getBaseCuboidId();
        if ((cubeStatsReader.getCuboidRowEstimatesHLL().get(baseCuboid) == null
                || cubeStatsReader.getCuboidRowEstimatesHLL().get(baseCuboid) == 0L)
                && segment.getConfig().isBuildBaseCuboid()) {
            logger.info(BASE_CUBOID_COUNT_IN_CUBOID_STATISTICS_IS_ZERO);
            return null;
        }

        String key = cube.getName() + "-" + segment.getName();
        CuboidStats cuboidStats = new CuboidStats.Builder(key, baseCuboid, cubeStatsReader.getCuboidRowEstimatesHLL(),
                cubeStatsReader.getCuboidSizeMap()).setHitFrequencyMap(hitFrequencyMap)
                        .setRollingUpCountSourceMap(rollingUpCountSourceMap).build();
        return CuboidRecommender.getInstance().getRecommendCuboidList(cuboidStats, segment.getConfig(),
                ifForceRecommend);
    }
}

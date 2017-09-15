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

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.algorithm.CuboidRecommender;
import org.apache.kylin.cube.cuboid.algorithm.CuboidStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuboidRecommenderUtil {

    private static final Logger logger = LoggerFactory.getLogger(CuboidRecommenderUtil.class);

    /** Trigger cube planner phase one */
    public static Map<Long, Long> getRecommendCuboidList(CubeSegment segment) throws IOException {
        if (segment == null) {
            return null;
        }

        CubeStatsReader cubeStatsReader = new CubeStatsReader(segment, segment.getConfig());
        if (cubeStatsReader.getCuboidRowEstimatesHLL() == null
                || cubeStatsReader.getCuboidRowEstimatesHLL().isEmpty()) {
            logger.info("Cuboid Statistics is not enabled.");
            return null;
        }
        long baseCuboid = segment.getCuboidScheduler().getBaseCuboidId();
        if (cubeStatsReader.getCuboidRowEstimatesHLL().get(baseCuboid) == null
                || cubeStatsReader.getCuboidRowEstimatesHLL().get(baseCuboid) == 0L) {
            logger.info("Base cuboid count in cuboid statistics is 0.");
            return null;
        }

        String key = segment.getCubeInstance().getName();
        CuboidStats cuboidStats = new CuboidStats.Builder(key, baseCuboid, cubeStatsReader.getCuboidRowEstimatesHLL(),
                cubeStatsReader.getCuboidSizeMap()).build();
        return CuboidRecommender.getInstance().getRecommendCuboidList(cuboidStats, segment.getConfig(), false);
    }

    /** Trigger cube planner phase two for optimization */
    public static Map<Long, Long> getRecommendCuboidList(CubeInstance cube, Map<Long, Long> hitFrequencyMap,
            Map<Long, Map<Long, Long>> rollingUpCountSourceMap) throws IOException {

        Pair<Map<Long, Long>, Map<Long, Double>> statsPair = CuboidStatsReaderUtil
                .readCuboidStatsAndSizeFromCube(cube.getCuboidScheduler().getAllCuboidIds(), cube);

        String key = cube.getName();
        long baseCuboid = cube.getCuboidScheduler().getBaseCuboidId();
        CuboidStats cuboidStats = new CuboidStats.Builder(key, baseCuboid, statsPair.getFirst(), statsPair.getSecond())
                .setHitFrequencyMap(hitFrequencyMap).setRollingUpCountSourceMap(rollingUpCountSourceMap,
                        cube.getConfig().getCubePlannerMandatoryRollUpThreshold())
                .build();
        return CuboidRecommender.getInstance().getRecommendCuboidList(cuboidStats, cube.getConfig());
    }

    /** For future segment level recommend */
    public static Map<Long, Long> getRecommendCuboidList(CubeSegment segment, Map<Long, Long> hitFrequencyMap,
            Map<Long, Map<Long, Long>> rollingUpCountSourceMap, boolean ifForceRecommend) throws IOException {
        if (segment == null) {
            return null;
        }

        CubeStatsReader cubeStatsReader = new CubeStatsReader(segment, segment.getConfig());
        if (cubeStatsReader.getCuboidRowEstimatesHLL() == null
                || cubeStatsReader.getCuboidRowEstimatesHLL().isEmpty()) {
            logger.info("Cuboid Statistics is not enabled.");
            return null;
        }
        long baseCuboid = segment.getCuboidScheduler().getBaseCuboidId();
        if (cubeStatsReader.getCuboidRowEstimatesHLL().get(baseCuboid) == null
                || cubeStatsReader.getCuboidRowEstimatesHLL().get(baseCuboid) == 0L) {
            logger.info("Base cuboid count in cuboid statistics is 0.");
            return null;
        }

        String key = segment.getCubeInstance().getName() + "-" + segment.getName();
        CuboidStats cuboidStats = new CuboidStats.Builder(key, baseCuboid, cubeStatsReader.getCuboidRowEstimatesHLL(),
                cubeStatsReader.getCuboidSizeMap()).setHitFrequencyMap(hitFrequencyMap)
                        .setRollingUpCountSourceMap(rollingUpCountSourceMap,
                                segment.getConfig().getCubePlannerMandatoryRollUpThreshold())
                        .build();
        return CuboidRecommender.getInstance().getRecommendCuboidList(cuboidStats, segment.getConfig(),
                ifForceRecommend);
    }
}

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticsDecisionUtil {
    protected static final Logger logger = LoggerFactory.getLogger(StatisticsDecisionUtil.class);

    public static void decideCubingAlgorithm(CubingJob cubingJob, CubeSegment seg) throws IOException {
        CubeStatsReader cubeStats = new CubeStatsReader(seg, null, seg.getConfig());
        decideCubingAlgorithm(cubingJob, seg, cubeStats.getMapperOverlapRatioOfFirstBuild(),
                cubeStats.getMapperNumberOfFirstBuild());
    }

    public static void decideCubingAlgorithm(CubingJob cubingJob, CubeSegment seg, double mapperOverlapRatio,
            int mapperNumber) throws IOException {
        KylinConfig kylinConf = seg.getConfig();
        String algPref = kylinConf.getCubeAlgorithm();
        CubingJob.AlgorithmEnum alg;
        if (mapperOverlapRatio == 0 && kylinConf.isAutoInmemToOptimize()) { // no source records
            alg = CubingJob.AlgorithmEnum.INMEM;
        } else if (CubingJob.AlgorithmEnum.INMEM.name().equalsIgnoreCase(algPref)) {
            alg = CubingJob.AlgorithmEnum.INMEM;
            if (seg.getCubeDesc().isStreamingCube() && CubingJob.CubingJobTypeEnum
                    .getByName(cubingJob.getJobType()) == CubingJob.CubingJobTypeEnum.BUILD) {
                alg = CubingJob.AlgorithmEnum.LAYER;
            }
        } else if (CubingJob.AlgorithmEnum.LAYER.name().equalsIgnoreCase(algPref)) {
            alg = CubingJob.AlgorithmEnum.LAYER;
        } else {
            int memoryHungryMeasures = 0;
            for (MeasureDesc measure : seg.getCubeDesc().getMeasures()) {
                if (measure.getFunction().getMeasureType().isMemoryHungry()) {
                    logger.info("This cube has memory-hungry measure " + measure.getFunction().getExpression());
                    memoryHungryMeasures++;
                }
            }

            if (memoryHungryMeasures > 0) {
                alg = CubingJob.AlgorithmEnum.LAYER;
            } else if ("random".equalsIgnoreCase(algPref)) { // for testing
                alg = new Random().nextBoolean() ? CubingJob.AlgorithmEnum.INMEM : CubingJob.AlgorithmEnum.LAYER;
            } else { // the default
                int mapperNumLimit = kylinConf.getCubeAlgorithmAutoMapperLimit();
                double overlapThreshold = kylinConf.getCubeAlgorithmAutoThreshold();
                logger.info("mapperNumber for " + seg + " is " + mapperNumber + " and threshold is " + mapperNumLimit);
                logger.info("mapperOverlapRatio for " + seg + " is " + mapperOverlapRatio + " and threshold is "
                        + overlapThreshold);

                // in-mem cubing is good when
                // 1) the cluster has enough mapper slots to run in parallel
                // 2) the mapper overlap ratio is small, meaning the shuffle of in-mem MR has advantage
                alg = (mapperNumber <= mapperNumLimit && mapperOverlapRatio <= overlapThreshold)//
                        ? CubingJob.AlgorithmEnum.INMEM
                        : CubingJob.AlgorithmEnum.LAYER;
            }

        }
        logger.info("The cube algorithm for " + seg + " is " + alg);

        cubingJob.setAlgorithm(alg);
    }

    // For triggering cube planner phase one
    public static Map<Long, Long> optimizeCubingPlan(CubeSegment segment) throws IOException {
        if (isAbleToOptimizeCubingPlan(segment)) {
            logger.info("It's able to trigger cuboid planner algorithm.");
        } else {
            return new HashMap<>();
        }

        Map<Long, Long> recommendCuboidsWithStats = CuboidRecommenderUtil.getRecommendCuboidList(segment);
        if (recommendCuboidsWithStats == null || recommendCuboidsWithStats.isEmpty()) {
            return new HashMap<>();
        }

        CubeInstance cube = segment.getCubeInstance();
        CubeUpdate update = new CubeUpdate(cube.latestCopyForWrite());
        update.setCuboids(recommendCuboidsWithStats);
        CubeManager.getInstance(cube.getConfig()).updateCube(update);
        return recommendCuboidsWithStats;
    }

    public static boolean isAbleToOptimizeCubingPlan(CubeSegment segment) {
        CubeInstance cube = segment.getCubeInstance();
        if (!cube.getConfig().isCubePlannerEnabled())
            return false;

        if (cube.getSegments(SegmentStatusEnum.READY_PENDING).size() > 0) {
            logger.info("Has read pending segments and will not enable cube planner.");
            return false;
        }
        List<CubeSegment> readySegments = cube.getSegments(SegmentStatusEnum.READY);
        List<CubeSegment> newSegments = cube.getSegments(SegmentStatusEnum.NEW);
        if (newSegments.size() <= 1 && //
                (readySegments.size() == 0 || //
                        (cube.getConfig().isCubePlannerEnabledForExistingCube() && readySegments.size() == 1
                                && readySegments.get(0).getSegRange().equals(segment.getSegRange())))) {
            return true;
        } else {
            return false;
        }
    }
}

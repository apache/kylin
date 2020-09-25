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

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.job.exception.JobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class MapReduceUtil {

    private static final Logger logger = LoggerFactory.getLogger(MapReduceUtil.class);

    /**
     * @return reducer number for calculating hll
     */
    public static int getCuboidHLLCounterReducerNum(CubeInstance cube) {
        int nCuboids = cube.getCuboidScheduler().getAllCuboidIds().size();
        int shardBase = (nCuboids - 1) / cube.getConfig().getHadoopJobPerReducerHLLCuboidNumber() + 1;

        int hllMaxReducerNumber = cube.getConfig().getHadoopJobHLLMaxReducerNumber();
        if (shardBase > hllMaxReducerNumber) {
            shardBase = hllMaxReducerNumber;
        }
        return shardBase;
    }

    /**
     * @param cuboidScheduler specified can provide more flexibility
     * */
    public static int getLayeredCubingReduceTaskNum(CubeSegment cubeSegment, CuboidScheduler cuboidScheduler,
            double totalMapInputMB, int level)
            throws ClassNotFoundException, IOException, InterruptedException, JobException {
        CubeDesc cubeDesc = cubeSegment.getCubeDesc();
        KylinConfig kylinConfig = cubeDesc.getConfig();

        double perReduceInputMB = kylinConfig.getDefaultHadoopJobReducerInputMB();
        double reduceCountRatio = kylinConfig.getDefaultHadoopJobReducerCountRatio();
        logger.info("Having per reduce MB " + perReduceInputMB + ", reduce count ratio " + reduceCountRatio + ", level "
                + level);

        CubeStatsReader cubeStatsReader = new CubeStatsReader(cubeSegment, cuboidScheduler, kylinConfig);

        double parentLayerSizeEst, currentLayerSizeEst, adjustedCurrentLayerSizeEst;

        if (level == -1) {
            //merge case
            double estimatedSize = cubeStatsReader.estimateCubeSize();
            adjustedCurrentLayerSizeEst = estimatedSize > totalMapInputMB ? totalMapInputMB : estimatedSize;
            logger.debug("estimated size {}, input size {}, adjustedCurrentLayerSizeEst: {}", estimatedSize,
                    totalMapInputMB, adjustedCurrentLayerSizeEst);
        } else if (level == 0) {
            //base cuboid case TODO: the estimation could be very WRONG because it has no correction
            adjustedCurrentLayerSizeEst = cubeStatsReader.estimateLayerSize(0);
            logger.debug("adjustedCurrentLayerSizeEst: {}", adjustedCurrentLayerSizeEst);
        } else {
            parentLayerSizeEst = cubeStatsReader.estimateLayerSize(level - 1);
            currentLayerSizeEst = cubeStatsReader.estimateLayerSize(level);
            adjustedCurrentLayerSizeEst = totalMapInputMB / parentLayerSizeEst * currentLayerSizeEst;
            logger.debug(
                    "totalMapInputMB: {}, parentLayerSizeEst: {}, currentLayerSizeEst: {}, adjustedCurrentLayerSizeEst: {}",
                    totalMapInputMB, parentLayerSizeEst, currentLayerSizeEst, adjustedCurrentLayerSizeEst);
        }

        // number of reduce tasks
        int numReduceTasks = (int) Math.round(adjustedCurrentLayerSizeEst / perReduceInputMB * reduceCountRatio + 0.99);

        // adjust reducer number for cube which has DISTINCT_COUNT measures for better performance
        if (cubeDesc.hasMemoryHungryMeasures()) {
            logger.debug("Multiply reducer num by 4 to boost performance for memory hungry measures");
            numReduceTasks = numReduceTasks * 4;
        }

        // at least 1 reducer by default
        numReduceTasks = Math.max(kylinConfig.getHadoopJobMinReducerNumber(), numReduceTasks);
        // no more than 500 reducer by default
        numReduceTasks = Math.min(kylinConfig.getHadoopJobMaxReducerNumber(), numReduceTasks);

        return numReduceTasks;
    }

    public static int getInmemCubingReduceTaskNum(CubeSegment cubeSeg, CuboidScheduler cuboidScheduler)
            throws IOException {
        KylinConfig kylinConfig = cubeSeg.getConfig();

        Map<Long, Double> cubeSizeMap = new CubeStatsReader(cubeSeg, cuboidScheduler, kylinConfig).getCuboidSizeMap();
        double totalSizeInM = 0;
        for (Double cuboidSize : cubeSizeMap.values()) {
            totalSizeInM += cuboidSize;
        }
        return getReduceTaskNum(totalSizeInM, kylinConfig);
    }

    // @return the first indicates the total reducer number, the second indicates the reducer number for base cuboid
    public static Pair<Integer, Integer> getConvergeCuboidDataReduceTaskNums(CubeSegment cubeSeg) throws IOException {
        long baseCuboidId = cubeSeg.getCuboidScheduler().getBaseCuboidId();

        Set<Long> overlapCuboids = Sets.newHashSet(cubeSeg.getCuboidScheduler().getAllCuboidIds());
        overlapCuboids.retainAll(cubeSeg.getCubeInstance().getCuboidsRecommend());
        overlapCuboids.add(baseCuboidId);

        Pair<Map<Long, Long>, Long> cuboidStats = CuboidStatsReaderUtil
                .readCuboidStatsWithSourceFromSegment(overlapCuboids, cubeSeg);
        Map<Long, Double> cubeSizeMap = CubeStatsReader.getCuboidSizeMapFromRowCount(cubeSeg, cuboidStats.getFirst(),
                cuboidStats.getSecond());
        double totalSizeInM = 0;
        for (Double cuboidSize : cubeSizeMap.values()) {
            totalSizeInM += cuboidSize;
        }

        double baseSizeInM = cubeSizeMap.get(baseCuboidId);

        KylinConfig kylinConfig = cubeSeg.getConfig();
        int nBase = getReduceTaskNum(baseSizeInM, kylinConfig);
        int nOther = getReduceTaskNum(totalSizeInM - baseSizeInM, kylinConfig);
        return new Pair<>(nBase + nOther, nBase);
    }

    private static int getReduceTaskNum(double totalSizeInM, KylinConfig kylinConfig) {
        double perReduceInputMB = kylinConfig.getDefaultHadoopJobReducerInputMB();
        double reduceCountRatio = kylinConfig.getDefaultHadoopJobReducerCountRatio();

        // number of reduce tasks
        int numReduceTasks = (int) Math.round(totalSizeInM / perReduceInputMB * reduceCountRatio);

        // at least 1 reducer by default
        numReduceTasks = Math.max(kylinConfig.getHadoopJobMinReducerNumber(), numReduceTasks);
        // no more than 500 reducer by default
        numReduceTasks = Math.min(kylinConfig.getHadoopJobMaxReducerNumber(), numReduceTasks);

        logger.info("Having total map input MB " + Math.round(totalSizeInM));
        logger.info("Having per reduce MB " + perReduceInputMB);
        logger.info("Setting " + Reducer.Context.NUM_REDUCES + "=" + numReduceTasks);
        return numReduceTasks;
    }
}

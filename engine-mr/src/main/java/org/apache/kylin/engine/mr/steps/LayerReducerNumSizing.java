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

package org.apache.kylin.engine.mr.steps;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.job.exception.JobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LayerReducerNumSizing {

    private static final Logger logger = LoggerFactory.getLogger(LayerReducerNumSizing.class);

    public static int getReduceTaskNum(CubeSegment cubeSegment, double totalMapInputMB, int level) throws ClassNotFoundException, IOException, InterruptedException, JobException {
        CubeDesc cubeDesc = cubeSegment.getCubeDesc();
        KylinConfig kylinConfig = cubeDesc.getConfig();

        double perReduceInputMB = kylinConfig.getDefaultHadoopJobReducerInputMB();
        double reduceCountRatio = kylinConfig.getDefaultHadoopJobReducerCountRatio();
        logger.info("Having per reduce MB " + perReduceInputMB + ", reduce count ratio " + reduceCountRatio + ", level " + level);

        CubeStatsReader cubeStatsReader = new CubeStatsReader(cubeSegment, kylinConfig);

        double parentLayerSizeEst, currentLayerSizeEst, adjustedCurrentLayerSizeEst;

        if (level == -1) {
            //merge case
            double estimatedSize = cubeStatsReader.estimateCubeSize();
            adjustedCurrentLayerSizeEst = estimatedSize > totalMapInputMB ? totalMapInputMB : estimatedSize;
            logger.info("estimated size {}, input size {}, adjustedCurrentLayerSizeEst: {}", estimatedSize, totalMapInputMB, adjustedCurrentLayerSizeEst);
        } else if (level == 0) {
            //base cuboid case TODO: the estimation could be very WRONG because it has no correction
            adjustedCurrentLayerSizeEst = cubeStatsReader.estimateLayerSize(0);
            logger.info("adjustedCurrentLayerSizeEst: {}", adjustedCurrentLayerSizeEst);
        } else {
            parentLayerSizeEst = cubeStatsReader.estimateLayerSize(level - 1);
            currentLayerSizeEst = cubeStatsReader.estimateLayerSize(level);
            adjustedCurrentLayerSizeEst = totalMapInputMB / parentLayerSizeEst * currentLayerSizeEst;
            logger.info("totalMapInputMB: {}, parentLayerSizeEst: {}, currentLayerSizeEst: {}, adjustedCurrentLayerSizeEst: {}", totalMapInputMB, parentLayerSizeEst, currentLayerSizeEst, adjustedCurrentLayerSizeEst);
        }

        // number of reduce tasks
        int numReduceTasks = (int) Math.round(adjustedCurrentLayerSizeEst / perReduceInputMB * reduceCountRatio + 0.99);

        // adjust reducer number for cube which has DISTINCT_COUNT measures for better performance
        if (cubeDesc.hasMemoryHungryMeasures()) {
            logger.info("Multiply reducer num by 4 to boost performance for memory hungry measures");
            numReduceTasks = numReduceTasks * 4;
        }

        // at least 1 reducer by default
        numReduceTasks = Math.max(kylinConfig.getHadoopJobMinReducerNumber(), numReduceTasks);
        // no more than 500 reducer by default
        numReduceTasks = Math.min(kylinConfig.getHadoopJobMaxReducerNumber(), numReduceTasks);

        return numReduceTasks;
    }

}

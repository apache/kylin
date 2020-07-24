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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.engine.mr.common.CubeStatsWriter;
import org.apache.kylin.engine.mr.common.StatisticsDecisionUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 * Save the cube segment statistic to Kylin metadata store
 */
public class SaveStatisticsStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(SaveStatisticsStep.class);

    public SaveStatisticsStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        CubeSegment newSegment = CubingExecutableUtil.findSegment(context,
                CubingExecutableUtil.getCubeName(this.getParams()),
                CubingExecutableUtil.getSegmentId(this.getParams()));
        KylinConfig kylinConf = newSegment.getConfig();

        ResourceStore rs = ResourceStore.getStore(kylinConf);
        try {

            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            Configuration hadoopConf = HadoopUtil.getCurrentConfiguration();
            Path statisticsDir = new Path(CubingExecutableUtil.getStatisticsPath(this.getParams()));
            Path[] statisticsFiles = HadoopUtil.getFilteredPath(fs, statisticsDir,
                    BatchConstants.CFG_OUTPUT_STATISTICS);
            if (statisticsFiles == null) {
                throw new IOException("fail to find the statistics file in base dir: " + statisticsDir);
            }

            Map<Long, HLLCounter> cuboidHLLMap = Maps.newHashMap();
            long totalRowsBeforeMerge = 0;
            long grantTotal = 0;
            int samplingPercentage = -1;
            int mapperNumber = -1;
            for (Path item : statisticsFiles) {
                CubeStatsReader.CubeStatsResult cubeStatsResult = new CubeStatsReader.CubeStatsResult(item,
                        kylinConf.getCubeStatsHLLPrecision());
                cuboidHLLMap.putAll(cubeStatsResult.getCounterMap());
                long pGrantTotal = 0L;
                for (HLLCounter hll : cubeStatsResult.getCounterMap().values()) {
                    pGrantTotal += hll.getCountEstimate();
                }
                totalRowsBeforeMerge += pGrantTotal * cubeStatsResult.getMapperOverlapRatio();
                grantTotal += pGrantTotal;
                int pMapperNumber = cubeStatsResult.getMapperNumber();
                if (pMapperNumber > 0) {
                    if (mapperNumber < 0) {
                        mapperNumber = pMapperNumber;
                    } else {
                        throw new RuntimeException(
                                "Base cuboid has been distributed to multiple reducers at step FactDistinctColumnsReducer!!!");
                    }
                }
                int pSamplingPercentage = cubeStatsResult.getPercentage();
                if (samplingPercentage < 0) {
                    samplingPercentage = pSamplingPercentage;
                } else if (samplingPercentage != pSamplingPercentage) {
                    throw new RuntimeException(
                            "The sampling percentage should be same among all of the reducer of FactDistinctColumnsReducer!!!");
                }
            }
            if (samplingPercentage < 0) {
                logger.warn("The sampling percentage should be set!!!");
            }
            if (mapperNumber < 0) {
                logger.warn("The mapper number should be set!!!");
            }

            if (logger.isDebugEnabled()) {
                logMapperAndCuboidStatistics(cuboidHLLMap, samplingPercentage, mapperNumber, grantTotal,
                        totalRowsBeforeMerge);
            }
            double mapperOverlapRatio = grantTotal == 0 ? 0 : (double) totalRowsBeforeMerge / grantTotal;
            CubingJob cubingJob = (CubingJob) getManager()
                    .getJob(CubingExecutableUtil.getCubingJobId(this.getParams()));
            long sourceRecordCount = cubingJob.findSourceRecordCount();
            CubeStatsWriter.writeCuboidStatistics(hadoopConf, statisticsDir, cuboidHLLMap, samplingPercentage,
                    mapperNumber, mapperOverlapRatio, sourceRecordCount);

            Path statisticsFile = new Path(statisticsDir, BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION_FILENAME);
            logger.info("{} stats saved to hdfs {}", newSegment, statisticsFile);

            FSDataInputStream is = fs.open(statisticsFile);
            try {
                // put the statistics to metadata store
                String resPath = newSegment.getStatisticsResourcePath();
                rs.putResource(resPath, is, System.currentTimeMillis());
                logger.info("{} stats saved to resource {}", newSegment, resPath);

                StatisticsDecisionUtil.decideCubingAlgorithm(cubingJob, newSegment);
                StatisticsDecisionUtil.optimizeCubingPlan(newSegment);
            } finally {
                IOUtils.closeStream(is);
            }

            return ExecuteResult.createSucceed();
        } catch (IOException e) {
            logger.error("fail to save cuboid statistics", e);
            return ExecuteResult.createError(e);
        }
    }

    private void logMapperAndCuboidStatistics(Map<Long, HLLCounter> cuboidHLLMap, int samplingPercentage,
            int mapperNumber, long grantTotal, long totalRowsBeforeMerge) {
        logger.debug("Total cuboid number: \t" + cuboidHLLMap.size());
        logger.debug("Sampling percentage: \t" + samplingPercentage);
        logger.debug("The following statistics are collected based on sampling data.");
        logger.debug("Number of Mappers: " + mapperNumber);

        List<Long> allCuboids = Lists.newArrayList(cuboidHLLMap.keySet());
        Collections.sort(allCuboids);
        for (long i : allCuboids) {
            logger.debug("Cuboid " + i + " row count is: \t " + cuboidHLLMap.get(i).getCountEstimate());
        }

        logger.debug("Sum of all the cube segments (before merge) is: \t " + totalRowsBeforeMerge);
        logger.debug("After merge, the cube has row count: \t " + grantTotal);
        if (grantTotal > 0) {
            logger.debug("The mapper overlap ratio is: \t" + (double) totalRowsBeforeMerge / grantTotal);
        }
    }
}

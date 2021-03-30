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
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
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

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class MergeStatisticsWithOldStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(MergeStatisticsWithOldStep.class);

    protected Map<Long, HLLCounter> cuboidHLLMap = Maps.newHashMap();

    public MergeStatisticsWithOldStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeManager mgr = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = mgr.getCube(CubingExecutableUtil.getCubeName(this.getParams()));
        final CubeSegment optimizeSegment = cube.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));

        CubeSegment oldSegment = optimizeSegment.getCubeInstance().getOriginalSegmentToOptimize(optimizeSegment);
        Preconditions.checkNotNull(oldSegment,
                "cannot find the original segment to be optimized by " + optimizeSegment);

        KylinConfig kylinConf = cube.getConfig();
        Configuration conf = HadoopUtil.getCurrentConfiguration();
        ResourceStore rs = ResourceStore.getStore(kylinConf);
        int averageSamplingPercentage = 0;

        try {
            //1. Add statistics from optimized segment
            Path statisticsDirPath = new Path(CubingExecutableUtil.getStatisticsPath(this.getParams()));
            FileSystem hdfs = FileSystem.get(conf);
            if (!hdfs.exists(statisticsDirPath)) {
                throw new IOException("StatisticsFilePath " + statisticsDirPath + " does not exists");
            }

            if (!hdfs.isDirectory(statisticsDirPath)) {
                throw new IOException("StatisticsFilePath " + statisticsDirPath + " is not a directory");
            }

            Path[] statisticsFiles = HadoopUtil.getFilteredPath(hdfs, statisticsDirPath,
                    BatchConstants.CFG_OUTPUT_STATISTICS);
            if (statisticsFiles == null) {
                throw new IOException("fail to find the statistics file in base dir: " + statisticsDirPath);
            }

            for (Path item : statisticsFiles) {
                CubeStatsReader optimizeSegmentStatsReader = new CubeStatsReader(optimizeSegment, null,
                        optimizeSegment.getConfig(), item);
                averageSamplingPercentage += optimizeSegmentStatsReader.getSamplingPercentage();
                addFromCubeStatsReader(optimizeSegmentStatsReader);
            }

            //2. Add statistics from old segment
            CubeStatsReader oldSegmentStatsReader = new CubeStatsReader(oldSegment, null, oldSegment.getConfig());
            averageSamplingPercentage += oldSegmentStatsReader.getSamplingPercentage();
            addFromCubeStatsReader(oldSegmentStatsReader);

            logger.info("Cuboid set with stats info: " + cuboidHLLMap.keySet().toString());
            //3. Store merged statistics for recommend cuboids
            averageSamplingPercentage = averageSamplingPercentage / 2;
            Set<Long> cuboidsRecommend = cube.getCuboidsRecommend();

            Map<Long, HLLCounter> resultCuboidHLLMap = Maps.newHashMapWithExpectedSize(cuboidsRecommend.size());
            for (Long cuboid : cuboidsRecommend) {
                HLLCounter hll = cuboidHLLMap.get(cuboid);
                if (hll == null) {
                    logger.warn("Cannot get the row count stats for cuboid " + cuboid);
                } else {
                    resultCuboidHLLMap.put(cuboid, hll);
                }
            }

            String resultDir = CubingExecutableUtil.getMergedStatisticsPath(this.getParams());
            CubeStatsWriter.writeCuboidStatistics(conf, new Path(resultDir), resultCuboidHLLMap,
                    averageSamplingPercentage, oldSegmentStatsReader.getSourceRowCount());

            try (FSDataInputStream mergedStats = hdfs
                    .open(new Path(resultDir, BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION_FILENAME))) {
                // put the statistics to metadata store
                String statisticsFileName = optimizeSegment.getStatisticsResourcePath();
                rs.putResource(statisticsFileName, mergedStats, System.currentTimeMillis());
            }

            //By default, the cube optimization will use in-memory cubing
            CubingJob cubingJob = (CubingJob) getManager()
                    .getJob(CubingExecutableUtil.getCubingJobId(this.getParams()));
            StatisticsDecisionUtil.decideCubingAlgorithm(cubingJob, optimizeSegment);

            return new ExecuteResult();
        } catch (IOException e) {
            logger.error("fail to merge cuboid statistics", e);
            return ExecuteResult.createError(e);
        }

    }

    private void addFromCubeStatsReader(CubeStatsReader cubeStatsReader) {
        for (Map.Entry<Long, HLLCounter> entry : cubeStatsReader.getCuboidRowHLLCounters().entrySet()) {
            if (cuboidHLLMap.get(entry.getKey()) != null) {
                cuboidHLLMap.get(entry.getKey()).merge(entry.getValue());
            } else {
                cuboidHLLMap.put(entry.getKey(), entry.getValue());
            }
        }
    }

}

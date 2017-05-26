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
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.CubingJob.AlgorithmEnum;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        CubeSegment newSegment = CubingExecutableUtil.findSegment(context, CubingExecutableUtil.getCubeName(this.getParams()), CubingExecutableUtil.getSegmentId(this.getParams()));
        KylinConfig kylinConf = newSegment.getConfig();

        ResourceStore rs = ResourceStore.getStore(kylinConf);
        try {
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            Path statisticsDir = new Path(CubingExecutableUtil.getStatisticsPath(this.getParams()));
            Path statisticsFilePath = HadoopUtil.getFilterOnlyPath(fs, statisticsDir, BatchConstants.CFG_OUTPUT_STATISTICS);
            if (statisticsFilePath == null) {
                throw new IOException("fail to find the statistics file in base dir: " + statisticsDir);
            }

            FSDataInputStream is = fs.open(statisticsFilePath);
            try {
                // put the statistics to metadata store
                String statisticsFileName = newSegment.getStatisticsResourcePath();
                rs.putResource(statisticsFileName, is, System.currentTimeMillis());
            } finally {
                IOUtils.closeStream(is);
            }

            decideCubingAlgorithm(newSegment, kylinConf);

            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (IOException e) {
            logger.error("fail to save cuboid statistics", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

    private void decideCubingAlgorithm(CubeSegment seg, KylinConfig kylinConf) throws IOException {
        String algPref = kylinConf.getCubeAlgorithm();
        AlgorithmEnum alg;
        if (AlgorithmEnum.INMEM.name().equalsIgnoreCase(algPref)) {
            alg = AlgorithmEnum.INMEM;
        } else if (AlgorithmEnum.LAYER.name().equalsIgnoreCase(algPref)) {
            alg = AlgorithmEnum.LAYER;
        } else {
            int memoryHungryMeasures = 0;
            for (MeasureDesc measure : seg.getCubeDesc().getMeasures()) {
                if (measure.getFunction().getMeasureType().isMemoryHungry()) {
                    logger.info("This cube has memory-hungry measure " + measure.getFunction().getExpression());
                    memoryHungryMeasures++;
                }
            }

            if (memoryHungryMeasures > 0) {
                alg = AlgorithmEnum.LAYER;
            } else if ("random".equalsIgnoreCase(algPref)) { // for testing
                alg = new Random().nextBoolean() ? AlgorithmEnum.INMEM : AlgorithmEnum.LAYER;
            } else { // the default
                CubeStatsReader cubeStats = new CubeStatsReader(seg, kylinConf);
                int mapperNumber = cubeStats.getMapperNumberOfFirstBuild();
                int mapperNumLimit = kylinConf.getCubeAlgorithmAutoMapperLimit();
                double mapperOverlapRatio = cubeStats.getMapperOverlapRatioOfFirstBuild();
                double overlapThreshold = kylinConf.getCubeAlgorithmAutoThreshold();
                logger.info("mapperNumber for " + seg + " is " + mapperNumber + " and threshold is " + mapperNumLimit);
                logger.info("mapperOverlapRatio for " + seg + " is " + mapperOverlapRatio + " and threshold is " + overlapThreshold);

                // in-mem cubing is good when
                // 1) the cluster has enough mapper slots to run in parallel
                // 2) the mapper overlap ratio is small, meaning the shuffle of in-mem MR has advantage
                alg = (mapperNumber <= mapperNumLimit && mapperOverlapRatio <= overlapThreshold)//
                        ? AlgorithmEnum.INMEM : AlgorithmEnum.LAYER;
            }

        }
        logger.info("The cube algorithm for " + seg + " is " + alg);

        CubingJob cubingJob = (CubingJob) getManager().getJob(CubingExecutableUtil.getCubingJobId(this.getParams()));
        cubingJob.setAlgorithm(alg);
    }

}

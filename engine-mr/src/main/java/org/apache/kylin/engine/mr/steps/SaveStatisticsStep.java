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
import org.apache.kylin.engine.mr.common.StatisticsDecisionUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
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
            Path statisticsDir = new Path(CubingExecutableUtil.getStatisticsPath(this.getParams()));
            FileSystem fs = HadoopUtil.getFileSystem(statisticsDir);
            Path statisticsFilePath = HadoopUtil.getFilterOnlyPath(fs, statisticsDir, BatchConstants.CFG_OUTPUT_STATISTICS);
            if (statisticsFilePath == null) {
                throw new IOException("fail to find the statistics file in base dir: " + statisticsDir);
            }

            FSDataInputStream is = fs.open(statisticsFilePath);
            try {
                // put the statistics to metadata store
                String statisticsFileName = newSegment.getStatisticsResourcePath();
                rs.putResource(statisticsFileName, is, System.currentTimeMillis());

                CubingJob cubingJob = (CubingJob) getManager()
                        .getJob(CubingExecutableUtil.getCubingJobId(this.getParams()));
                StatisticsDecisionUtil.decideCubingAlgorithm(cubingJob, newSegment);
                StatisticsDecisionUtil.optimizeCubingPlan(newSegment);
            } finally {
                IOUtils.closeStream(is);
            }

            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (IOException e) {
            logger.error("fail to save cuboid statistics", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

}

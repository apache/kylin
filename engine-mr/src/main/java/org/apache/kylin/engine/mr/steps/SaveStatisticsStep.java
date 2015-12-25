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
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.CubingJob.AlgorithmEnum;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;

/**
 * Save the cube segment statistic to Kylin metadata store
 *
 */
public class SaveStatisticsStep extends AbstractExecutable {

    private static final String CUBE_NAME = "cubeName";
    private static final String SEGMENT_ID = "segmentId";
    private static final String STATISTICS_PATH = "statisticsPath";
    private static final String CUBING_JOB_ID = "cubingJobId";

    public SaveStatisticsStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        KylinConfig kylinConf = context.getConfig();
        final CubeManager mgr = CubeManager.getInstance(kylinConf);
        final CubeInstance cube = mgr.getCube(getCubeName());
        final CubeSegment newSegment = cube.getSegmentById(getSegmentId());

        ResourceStore rs = ResourceStore.getStore(kylinConf);
        try {
            Path statisticsFilePath = new Path(getStatisticsPath(), BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION);
            FileSystem fs = FileSystem.get(HadoopUtil.getCurrentConfiguration());
            if (!fs.exists(statisticsFilePath))
                throw new IOException("File " + statisticsFilePath + " does not exists;");

            FSDataInputStream is = fs.open(statisticsFilePath);
            try {
                // put the statistics to metadata store
                String statisticsFileName = newSegment.getStatisticsResourcePath();
                rs.putResource(statisticsFileName, is, System.currentTimeMillis());
            } finally {
                IOUtils.closeStream(is);
                fs.delete(statisticsFilePath, true);
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
        } else if ("random".equalsIgnoreCase(algPref)) { // for testing
            alg = new Random().nextBoolean() ? AlgorithmEnum.INMEM : AlgorithmEnum.LAYER;
        } else { // the default
            double threshold = kylinConf.getCubeAlgorithmAutoThreshold();
            double mapperOverlapRatio = new CubeStatsReader(seg, kylinConf).getMapperOverlapRatioOfFirstBuild();
            logger.info("mapperOverlapRatio for " + seg + " is " + mapperOverlapRatio + " and threshold is " + threshold);
            alg = mapperOverlapRatio < threshold ? AlgorithmEnum.INMEM : AlgorithmEnum.LAYER;
        }

        logger.info("The cube algorithm for " + seg + " is " + alg);

        CubingJob cubingJob = (CubingJob) executableManager.getJob(getCubingJobId());
        cubingJob.setAlgorithm(alg);
    }

    public void setCubeName(String cubeName) {
        this.setParam(CUBE_NAME, cubeName);
    }

    private String getCubeName() {
        return getParam(CUBE_NAME);
    }

    public void setSegmentId(String segmentId) {
        this.setParam(SEGMENT_ID, segmentId);
    }

    private String getSegmentId() {
        return getParam(SEGMENT_ID);
    }

    public void setStatisticsPath(String path) {
        this.setParam(STATISTICS_PATH, path);
    }

    private String getStatisticsPath() {
        return getParam(STATISTICS_PATH);
    }

    public void setCubingJobId(String id) {
        setParam(CUBING_JOB_ID, id);
    }

    private String getCubingJobId() {
        return getParam(CUBING_JOB_ID);
    }

}

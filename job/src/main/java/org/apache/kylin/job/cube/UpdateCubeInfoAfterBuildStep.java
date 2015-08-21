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

package org.apache.kylin.job.cube;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.Output;

import java.io.IOException;

/**
 * Created by qianzhou on 1/4/15.
 */
public class UpdateCubeInfoAfterBuildStep extends AbstractExecutable {

    private static final String SEGMENT_ID = "segmentId";
    private static final String CUBE_NAME = "cubeName";
    private static final String CONVERT_TO_HFILE_STEP_ID = "convertToHFileStepId";
    private static final String BASE_CUBOID_STEP_ID = "baseCuboidStepId";
    private static final String CREATE_FLAT_TABLE_STEP_ID = "createFlatTableStepId";
    private static final String CUBING_JOB_ID = "cubingJobId";

    public UpdateCubeInfoAfterBuildStep() {
        super();
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

    public void setConvertToHFileStepId(String id) {
        setParam(CONVERT_TO_HFILE_STEP_ID, id);
    }

    private String getConvertToHfileStepId() {
        return getParam(CONVERT_TO_HFILE_STEP_ID);
    }

    public void setBaseCuboidStepId(String id) {
        setParam(BASE_CUBOID_STEP_ID, id);
    }

    private String getBaseCuboidStepId() {
        return getParam(BASE_CUBOID_STEP_ID);
    }

    public void setCreateFlatTableStepId(String id) {
        setParam(CREATE_FLAT_TABLE_STEP_ID, id);
    }

    public void setCubingJobId(String id) {
        setParam(CUBING_JOB_ID, id);
    }

    private String getCubingJobId() {
        return getParam(CUBING_JOB_ID);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeManager cubeManager = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = cubeManager.getCube(getCubeName());
        final CubeSegment segment = cube.getSegmentById(getSegmentId());

        Output baseCuboidOutput = executableManager.getOutput(getBaseCuboidStepId());
        String sourceRecordsCount = baseCuboidOutput.getExtra().get(ExecutableConstants.SOURCE_RECORDS_COUNT);
        long sourceCount = 0l;
        if (StringUtils.isNotEmpty(sourceRecordsCount)) {
            sourceCount = Long.parseLong(sourceRecordsCount);
        } else {
            logger.warn("Can not get cube source record count.");
        }

        long sourceSize = 0l;
        String sourceRecordsSize = baseCuboidOutput.getExtra().get(ExecutableConstants.SOURCE_RECORDS_SIZE);
        if (StringUtils.isNotEmpty(sourceRecordsSize)) {
            sourceSize = Long.parseLong(sourceRecordsSize);
        } else {
            logger.warn("Can not get cube source record size.");
        }

        long size = 0;
        boolean segmentReady = true;
        if (!StringUtils.isBlank(getConvertToHfileStepId())) {
            String cubeSizeString = executableManager.getOutput(getConvertToHfileStepId()).getExtra().get(ExecutableConstants.HDFS_BYTES_WRITTEN);
            if (StringUtils.isNotEmpty(cubeSizeString)) {
                size = Long.parseLong(cubeSizeString) / 1024;
            } else {
                logger.warn("Can't get cube segment size.");
            }
        } else {
            // for the increment & merge case, the increment segment is only built to be merged, won't serve query by itself
            segmentReady = false;
        }

        segment.setLastBuildJobID(getCubingJobId());
        segment.setLastBuildTime(System.currentTimeMillis());
        segment.setSizeKB(size);
        segment.setInputRecords(sourceCount);
        segment.setInputRecordsSize(sourceSize);

        try {
            if (segmentReady) {
                cubeManager.promoteNewlyBuiltSegments(cube, segment);
            } else {
                cubeManager.updateCube(cube);
            }
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (IOException e) {
            logger.error("fail to update cube after build", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }
}

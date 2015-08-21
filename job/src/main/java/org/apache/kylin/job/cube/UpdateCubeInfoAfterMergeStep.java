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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;

import com.google.common.collect.Lists;

/**
 * Created by qianzhou on 1/7/15.
 */
public class UpdateCubeInfoAfterMergeStep extends AbstractExecutable {

    private static final String CUBE_NAME = "cubeName";
    private static final String SEGMENT_ID = "segmentId";
    private static final String MERGING_SEGMENT_IDS = "mergingSegmentIds";
    private static final String CONVERT_TO_HFILE_STEP_ID = "convertToHFileStepId";
    private static final String CUBING_JOB_ID = "cubingJobId";

    private final CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());

    public UpdateCubeInfoAfterMergeStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeInstance cube = cubeManager.getCube(getCubeName());

        CubeSegment mergedSegment = cube.getSegmentById(getSegmentId());
        if (mergedSegment == null) {
            return new ExecuteResult(ExecuteResult.State.FAILED, "there is no segment with id:" + getSegmentId());
        }

        long cubeSize = 0l;
        String cubeSizeString = executableManager.getOutput(getConvertToHfileStepId()).getExtra().get(ExecutableConstants.HDFS_BYTES_WRITTEN);
        if (StringUtils.isNotEmpty(cubeSizeString)) {
            cubeSize = Long.parseLong(cubeSizeString) / 1024;
        } else {
            logger.warn("Can not get cube segment size.");
        }

        // collect source statistics
        List<String> mergingSegmentIds = getMergingSegmentIds();
        if (mergingSegmentIds.isEmpty()) {
            return new ExecuteResult(ExecuteResult.State.FAILED, "there are no merging segments");
        }
        long sourceCount = 0L;
        long sourceSize = 0L;
        for (String id : mergingSegmentIds) {
            CubeSegment segment = cube.getSegmentById(id);
            sourceCount += segment.getInputRecords();
            sourceSize += segment.getInputRecordsSize();
        }

        // update segment info
        mergedSegment.setSizeKB(cubeSize);
        mergedSegment.setInputRecords(sourceCount);
        mergedSegment.setInputRecordsSize(sourceSize);
        mergedSegment.setLastBuildJobID(getCubingJobId());
        mergedSegment.setLastBuildTime(System.currentTimeMillis());

        try {
            cubeManager.promoteNewlyBuiltSegments(cube, mergedSegment);
            return new ExecuteResult(ExecuteResult.State.SUCCEED);
        } catch (IOException e) {
            logger.error("fail to update cube after merge", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

    public void setSegmentId(String segmentId) {
        this.setParam(SEGMENT_ID, segmentId);
    }

    private String getSegmentId() {
        return getParam(SEGMENT_ID);
    }

    public void setCubeName(String cubeName) {
        this.setParam(CUBE_NAME, cubeName);
    }

    private String getCubeName() {
        return getParam(CUBE_NAME);
    }

    public void setMergingSegmentIds(List<String> ids) {
        setParam(MERGING_SEGMENT_IDS, StringUtils.join(ids, ","));
    }

    private List<String> getMergingSegmentIds() {
        final String ids = getParam(MERGING_SEGMENT_IDS);
        if (ids != null) {
            final String[] splitted = StringUtils.split(ids, ",");
            ArrayList<String> result = Lists.newArrayListWithExpectedSize(splitted.length);
            for (String id : splitted) {
                result.add(id);
            }
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    public void setConvertToHFileStepId(String id) {
        setParam(CONVERT_TO_HFILE_STEP_ID, id);
    }

    private String getConvertToHfileStepId() {
        return getParam(CONVERT_TO_HFILE_STEP_ID);
    }

    public void setCubingJobId(String id) {
        setParam(CUBING_JOB_ID, id);
    }

    private String getCubingJobId() {
        return getParam(CUBING_JOB_ID);
    }
}

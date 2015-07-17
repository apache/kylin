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

import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;

/**
 */
public class UpdateCubeInfoAfterBuildStep extends AbstractExecutable {

    private static final String SEGMENT_ID = "segmentId";
    private static final String CUBE_NAME = "cubeName";
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
        
        CubingJob cubingJob = (CubingJob) executableManager.getJob(getCubingJobId());
        long sourceCount = cubingJob.findSourceRecordCount();
        long sourceSizeBytes = cubingJob.findSourceSizeBytes();
        long cubeSizeBytes = cubingJob.findCubeSizeBytes();
        boolean segmentReady = cubeSizeBytes > 0; // for build+merge scenario, convert HFile not happen yet, so cube size is 0

        segment.setLastBuildJobID(getCubingJobId());
        segment.setLastBuildTime(System.currentTimeMillis());
        segment.setSizeKB(cubeSizeBytes / 1024);
        segment.setInputRecords(sourceCount);
        segment.setInputRecordsSize(sourceSizeBytes);

        try {
            if (segmentReady) {
                cubeManager.promoteNewlyBuiltSegments(cube, segment);
            } else {
                CubeUpdate cubeBuilder = new CubeUpdate(cube);
                cubeBuilder.setToUpdateSegs(segment);
                cubeManager.updateCube(cubeBuilder);
            }
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (IOException e) {
            logger.error("fail to update cube after build", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }
}

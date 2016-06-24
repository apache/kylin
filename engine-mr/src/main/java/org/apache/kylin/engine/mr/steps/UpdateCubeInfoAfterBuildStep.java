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

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class UpdateCubeInfoAfterBuildStep extends AbstractExecutable {
    private static final Logger logger = LoggerFactory.getLogger(UpdateCubeInfoAfterBuildStep.class);

    public UpdateCubeInfoAfterBuildStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeManager cubeManager = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = cubeManager.getCube(CubingExecutableUtil.getCubeName(this.getParams()));
        final CubeSegment segment = cube.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));

        CubingJob cubingJob = (CubingJob) executableManager.getJob(CubingExecutableUtil.getCubingJobId(this.getParams()));
        long sourceCount = cubingJob.findSourceRecordCount();
        long sourceSizeBytes = cubingJob.findSourceSizeBytes();
        long cubeSizeBytes = cubingJob.findCubeSizeBytes();

        segment.setLastBuildJobID(CubingExecutableUtil.getCubingJobId(this.getParams()));
        segment.setIndexPath(CubingExecutableUtil.getIndexPath(this.getParams()));
        segment.setLastBuildTime(System.currentTimeMillis());
        segment.setSizeKB(cubeSizeBytes / 1024);
        segment.setInputRecords(sourceCount);
        segment.setInputRecordsSize(sourceSizeBytes);

        try {
            cubeManager.promoteNewlyBuiltSegments(cube, segment);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (IOException e) {
            logger.error("fail to update cube after build", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

}

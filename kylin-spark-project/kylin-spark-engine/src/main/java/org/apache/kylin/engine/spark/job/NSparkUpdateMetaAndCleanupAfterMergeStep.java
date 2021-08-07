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

package org.apache.kylin.engine.spark.job;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.spark.metadata.cube.PathManager;
import org.apache.kylin.engine.spark.utils.UpdateMetadataUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.Segments;

public class NSparkUpdateMetaAndCleanupAfterMergeStep extends NSparkExecutable {
    public NSparkUpdateMetaAndCleanupAfterMergeStep() {
        this.setName(ExecutableConstants.STEP_NAME_MERGE_CLEANUP);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        String cubeId = getParam(MetadataConstants.P_CUBE_ID);
        String mergedSegmentUuid = getParam(CubingExecutableUtil.SEGMENT_ID);
        final KylinConfig config = wrapConfig(context);
        CubeInstance cube = CubeManager.getInstance(config).getCubeByUuid(cubeId);

        try {
            // update segments
            UpdateMetadataUtil.updateMetadataAfterMerge(cubeId, mergedSegmentUuid, config);
        } catch (IOException e) {
            throw new ExecuteException("Can not update metadata of cube: " + cube.getName());
        }

        if (config.cleanStorageAfterDelOperation()) {
            CubeSegment mergedSegment = cube.getSegmentById(mergedSegmentUuid);
            Segments<CubeSegment> mergingSegments = cube.getMergingSegments(mergedSegment);
            // delete segments which were merged
            for (CubeSegment segment : mergingSegments) {
                try {
                    PathManager.deleteSegmentParquetStoragePath(cube, segment.getName(), segment.getStorageLocationIdentifier());
                } catch (IOException e) {
                    throw new ExecuteException("Can not delete segment: " + segment.getName() + ", in cube: " + cube.getName());
                }
            }
        }
        return ExecuteResult.createSucceed();
    }

    @Override
    public void cleanup(ExecuteResult result) throws ExecuteException {
        // delete job tmp dir
        if (result != null && result.state().ordinal() == ExecuteResult.State.SUCCEED.ordinal()) {
            PathManager.deleteJobTempPath(getConfig(), getParam(MetadataConstants.P_PROJECT_NAME),
                    getParam(MetadataConstants.P_JOB_ID));
        }
    }
}

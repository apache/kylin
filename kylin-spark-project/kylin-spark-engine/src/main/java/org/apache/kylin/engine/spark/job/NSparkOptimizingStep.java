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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.spark.metadata.cube.PathManager;
import org.apache.kylin.engine.spark.utils.MetaDumpUtil;
import org.apache.kylin.engine.spark.utils.UpdateMetadataUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class NSparkOptimizingStep extends NSparkExecutable {
    private static final Logger logger = LoggerFactory.getLogger(NSparkOptimizingStep.class);

    // called by reflection
    public NSparkOptimizingStep() {
    }

    public NSparkOptimizingStep(String sparkSubmitClassName) {
        this.setSparkSubmitClassName(sparkSubmitClassName);
        this.setName(ExecutableConstants.STEP_NAME_BUILD_CUBOID_FROM_PARENT_CUBOID);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        String cubeId = getParam(MetadataConstants.P_CUBE_ID);
        CubeInstance cubeInstance = CubeManager.getInstance(config).getCubeByUuid(cubeId);
        return MetaDumpUtil.collectCubeMetadata(cubeInstance);
    }

    public static class Mockup {
        public static void main(String[] args) {
            logger.info(NSparkCubingStep.Mockup.class + ".main() invoked, args: " + Arrays.toString(args));
        }
    }

    @Override
    public boolean needMergeMetadata() {
        return true;
    }

    @Override
    protected Map<String, String> getJobMetricsInfo(KylinConfig config) {
        CubeManager cubeManager = CubeManager.getInstance(config);
        CubeInstance cube = cubeManager.getCube(getCubeName());
        Map<String, String> joblogInfo = Maps.newHashMap();
        joblogInfo.put(CubingJob.SOURCE_SIZE_BYTES, String.valueOf(cube.getInputRecordSizeBytes()));
        joblogInfo.put(CubingJob.CUBE_SIZE_BYTES, String.valueOf(cube.getSizeKB()));
        return joblogInfo;
    }

    @Override
    public void cleanup(ExecuteResult result) throws ExecuteException {
        // delete job tmp dir
        if (result != null && result.state().ordinal() == ExecuteResult.State.SUCCEED.ordinal()) {
            PathManager.deleteJobTempPath(getConfig(), getParam(MetadataConstants.P_PROJECT_NAME),
                    getParam(MetadataConstants.P_JOB_ID));
        }
    }

    @Override
    protected void updateMetaAfterOperation(KylinConfig config) throws IOException {
        UpdateMetadataUtil.syncLocalMetadataToRemote(config, this);
    }
}

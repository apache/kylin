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
package org.apache.kylin.storage.parquet.steps;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;

/**
 * Created by Yichen on 11/8/18.
 */
public class ParquetMRSteps extends ParquetJobSteps{
    public ParquetMRSteps(CubeSegment seg) {
        super(seg);
    }

    @Override
    public AbstractExecutable convertToParquetStep(String jobId) {
        String cuboidRootPath = getCuboidRootPath(jobId);
        String inputPath = cuboidRootPath + (cuboidRootPath.endsWith("/") ? "" : "/") + "*";

        final MapReduceExecutable mrExecutable = new MapReduceExecutable();
        mrExecutable.setName(ExecutableConstants.STEP_NAME_CONVERT_CUBOID_TO_PARQUET);
        mrExecutable.setMapReduceJobClass(MRCubeParquetJob.class);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, inputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getParquetOutputPath(jobId));
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Parquet_Generator_" + seg.getRealization().getName()+ "_Step");

        mrExecutable.setMapReduceParams(cmd.toString());
        mrExecutable.setName(ExecutableConstants.STEP_NAME_CONVERT_CUBOID_TO_PARQUET);
        mrExecutable.setCounterSaveAs(",," + CubingJob.CUBE_SIZE_BYTES);

        return mrExecutable;
    }

}

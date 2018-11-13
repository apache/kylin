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

import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeSegment;

import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.spark.SparkBatchCubingJobBuilder2;
import org.apache.kylin.engine.spark.SparkExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;


public class ParquetSparkSteps extends ParquetJobSteps {

    public ParquetSparkSteps(CubeSegment seg) {
        super(seg);
    }

    @Override
    public AbstractExecutable convertToParquetStep(String jobId) {

        String cuboidRootPath = getCuboidRootPath(jobId);
        String inputPath = cuboidRootPath + (cuboidRootPath.endsWith("/") ? "" : "/");

        SparkBatchCubingJobBuilder2 jobBuilder2 = new SparkBatchCubingJobBuilder2(seg, null);
        final SparkExecutable sparkExecutable = new SparkExecutable();
        sparkExecutable.setClassName(SparkCubeParquet.class.getName());
        sparkExecutable.setParam(SparkCubeParquet.OPTION_CUBE_NAME.getOpt(), seg.getRealization().getName());
        sparkExecutable.setParam(SparkCubeParquet.OPTION_SEGMENT_ID.getOpt(), seg.getUuid());
        sparkExecutable.setParam(SparkCubeParquet.OPTION_INPUT_PATH.getOpt(), inputPath);
        sparkExecutable.setParam(SparkCubeParquet.OPTION_OUTPUT_PATH.getOpt(), getParquetOutputPath(jobId));
        sparkExecutable.setParam(SparkCubeParquet.OPTION_META_URL.getOpt(),
                jobBuilder2.getSegmentMetadataUrl(seg.getConfig(), jobId));
        sparkExecutable.setJobId(jobId);

        StringBuilder jars = new StringBuilder();

        StringUtil.appendWithSeparator(jars, seg.getConfig().getSparkAdditionalJars());
        sparkExecutable.setJars(jars.toString());

        sparkExecutable.setName(ExecutableConstants.STEP_NAME_CONVERT_CUBOID_TO_PARQUET);
        sparkExecutable.setCounterSaveAs(",," + CubingJob.CUBE_SIZE_BYTES, getCounterOutputPath(jobId));

        return sparkExecutable;
    }


}

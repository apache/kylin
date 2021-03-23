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
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.spark.utils.MetaDumpUtil;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;
import java.util.Set;

public class NSparkOptimizingJob extends CubingJob {
    private static final Logger logger = LoggerFactory.getLogger(NSparkOptimizingJob.class);
    private static final String DEPLOY_ENV_NAME = "envName";

    public static NSparkOptimizingJob optimize(CubeSegment optimizedSegment, String submitter) {
        return NSparkOptimizingJob.optimize(optimizedSegment, submitter, CubingJobTypeEnum.OPTIMIZE, UUID.randomUUID().toString());
    }

    public static NSparkOptimizingJob optimize(CubeSegment optimizedSegment, String submitter, CubingJobTypeEnum jobType, String jobId) {
        logger.info("SPARK_V2 new job to OPTIMIZE a segment " + optimizedSegment);
        CubeSegment oldSegment = optimizedSegment.getCubeInstance().getOriginalSegmentToOptimize(optimizedSegment);
        Preconditions.checkNotNull(oldSegment, "cannot find the original segment to be optimized by " + optimizedSegment);
        CubeInstance cube = optimizedSegment.getCubeInstance();

        NSparkOptimizingJob job = new NSparkOptimizingJob();
        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss", Locale.ROOT);
        format.setTimeZone(TimeZone.getTimeZone(cube.getConfig().getTimeZone()));

        StringBuilder builder = new StringBuilder();
        builder.append(jobType).append(" CUBE - ");
        builder.append(optimizedSegment.getCubeInstance().getDisplayName()).append(" - ").append(optimizedSegment.getName())
                .append(" - ");

        builder.append(format.format(new Date(System.currentTimeMillis())));
        job.setName(builder.toString());
        job.setId(jobId);
        job.setTargetSubject(optimizedSegment.getModel().getUuid());
        job.setTargetSegments(Lists.newArrayList(String.valueOf(optimizedSegment.getUuid())));
        job.setProject(optimizedSegment.getProject());
        job.setSubmitter(submitter);

        job.setParam(MetadataConstants.P_JOB_ID, jobId);
        job.setParam(MetadataConstants.P_PROJECT_NAME, cube.getProject());
        job.setParam(MetadataConstants.P_TARGET_MODEL, job.getTargetSubject());
        job.setParam(MetadataConstants.P_CUBE_ID, cube.getId());
        job.setParam(MetadataConstants.P_CUBE_NAME, cube.getName());
        job.setParam(MetadataConstants.P_SEGMENT_IDS, String.join(",", job.getTargetSegments()));
        job.setParam(CubingExecutableUtil.SEGMENT_ID, optimizedSegment.getUuid());
        job.setParam(MetadataConstants.SEGMENT_NAME, optimizedSegment.getName());
        job.setParam(MetadataConstants.P_DATA_RANGE_START, optimizedSegment.getSegRange().start.toString());
        job.setParam(MetadataConstants.P_DATA_RANGE_END, optimizedSegment.getSegRange().end.toString());
        job.setParam(MetadataConstants.P_OUTPUT_META_URL, cube.getConfig().getMetadataUrl().toString());
        job.setParam(MetadataConstants.P_JOB_TYPE, String.valueOf(jobType));
        job.setParam(MetadataConstants.P_CUBOID_NUMBER, String.valueOf(cube.getDescriptor().getAllCuboids().size()));

        // Phase 1: Prepare base cuboid data from old segment
        JobStepFactory.addStep(job, JobStepType.FILTER_RECOMMEND_CUBOID, cube);

        // Phase 2: Resource detect
        JobStepFactory.addStep(job, JobStepType.RESOURCE_DETECT, cube);

        // Phase 3: Calculate cuboid statistics for optimized segment, Build Cube for Missing Cuboid Data, Update metadata
        JobStepFactory.addStep(job, JobStepType.OPTIMIZING, cube);

        return job;
    }

    @Override
    public Set<String> getMetadataDumpList(KylinConfig config) {
        String cubeId = getParam(MetadataConstants.P_CUBE_ID);
        CubeInstance cubeInstance = CubeManager.getInstance(config).getCubeByUuid(cubeId);
        return MetaDumpUtil.collectCubeMetadata(cubeInstance);
    }

    public String getDeployEnvName() {
        return getParam(DEPLOY_ENV_NAME);
    }
}

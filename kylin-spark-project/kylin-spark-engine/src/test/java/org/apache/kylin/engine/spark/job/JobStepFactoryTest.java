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
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

public class JobStepFactoryTest extends LocalWithSparkSessionTest {
    private KylinConfig config;
    private static final String CUBE_NAME = "ci_left_join_cube";

    @Override
    public void setup() throws SchedulerException {
        super.setup();
        config = getTestConfig();
    }

    @Override
    public void after() {
        super.after();
    }

    @Test
    public void testAddStepInCubing() throws IOException {
        CubeManager cubeMgr = CubeManager.getInstance(config);
        CubeInstance cube = cubeMgr.getCube(CUBE_NAME);
        cleanupSegments(CUBE_NAME);
        CubeSegment oneSeg = cubeMgr.appendSegment(cube, new SegmentRange.TSRange(0L, Long.MAX_VALUE));
        Set<CubeSegment> segments = Sets.newHashSet(oneSeg);
        NSparkCubingJob job = NSparkCubingJob.create(segments, "ADMIN");
        Assert.assertEquals(CUBE_NAME, job.getParam(MetadataConstants.P_CUBE_NAME));

        NSparkExecutable resourceDetectStep = job.getResourceDetectStep();
        Assert.assertEquals(ResourceDetectBeforeCubingJob.class.getName(),
                resourceDetectStep.getSparkSubmitClassName());
        Assert.assertEquals(ExecutableConstants.STEP_NAME_DETECT_RESOURCE, resourceDetectStep.getName());
        job.getParams().forEach((key, value) -> Assert.assertEquals(value, resourceDetectStep.getParam(key)));
        Assert.assertEquals(config.getJobTmpMetaStoreUrl(getProject(), resourceDetectStep.getId()).toString(),
                resourceDetectStep.getDistMetaUrl());

        NSparkExecutable cubeStep = job.getSparkCubingStep();
        Assert.assertEquals(config.getSparkBuildClassName(), cubeStep.getSparkSubmitClassName());
        Assert.assertEquals(ExecutableConstants.STEP_NAME_BUILD_SPARK_CUBE, cubeStep.getName());
        job.getParams().forEach((key, value) -> Assert.assertEquals(value, cubeStep.getParam(key)));
        Assert.assertEquals(config.getJobTmpMetaStoreUrl(getProject(), cubeStep.getId()).toString(),
                cubeStep.getDistMetaUrl());
    }

    @Test
    public void testAddStepInMerging() throws Exception {
        CubeManager cubeMgr = CubeManager.getInstance(config);
        CubeInstance cube = cubeMgr.getCube(CUBE_NAME);

        cleanupSegments(CUBE_NAME);
        /**
         * Round1. Add 2 segment
         */
        CubeSegment segment1 = cubeMgr.appendSegment(cube, new SegmentRange.TSRange(dateToLong("2010-01-01"), dateToLong("2013-01-01")));
        CubeSegment segment2 = cubeMgr.appendSegment(cube, new SegmentRange.TSRange(dateToLong("2013-01-01"), dateToLong("2015-01-01")));
        segment1.setStatus(SegmentStatusEnum.READY);
        segment2.setStatus(SegmentStatusEnum.READY);

        CubeInstance reloadCube = cube.latestCopyForWrite();
        Segments segments = new Segments();
        segments.add(segment1);
        segments.add(segment2);
        reloadCube.setSegments(segments);
        CubeUpdate update = new CubeUpdate(reloadCube);
        cubeMgr.updateCube(update);

        /**
         * Round2. Merge two segments
         */

        reloadCube = cubeMgr.reloadCube(CUBE_NAME);
        CubeSegment mergedSegment = cubeMgr.mergeSegments(reloadCube, new SegmentRange.TSRange(dateToLong("2010-01-01"), dateToLong("2015-01-01"))
                , null, true);
        NSparkMergingJob job = NSparkMergingJob.merge(mergedSegment, "ADMIN");
        Assert.assertEquals(CUBE_NAME, job.getParam(MetadataConstants.P_CUBE_NAME));

        NSparkExecutable resourceDetectStep = job.getResourceDetectStep();
        Assert.assertEquals(ResourceDetectBeforeMergingJob.class.getName(),
                resourceDetectStep.getSparkSubmitClassName());
        Assert.assertEquals(ExecutableConstants.STEP_NAME_DETECT_RESOURCE, resourceDetectStep.getName());
        job.getParams().forEach((key, value) -> Assert.assertEquals(value, resourceDetectStep.getParam(key)));
        Assert.assertEquals(config.getJobTmpMetaStoreUrl(getProject(), resourceDetectStep.getId()).toString(),
                resourceDetectStep.getDistMetaUrl());

        NSparkExecutable mergeStep = job.getSparkMergingStep();
        Assert.assertEquals(config.getSparkMergeClassName(), mergeStep.getSparkSubmitClassName());
        Assert.assertEquals(ExecutableConstants.STEP_NAME_MERGER_SPARK_SEGMENT, mergeStep.getName());
        job.getParams().forEach((key, value) -> Assert.assertEquals(value, mergeStep.getParam(key)));
        Assert.assertEquals(config.getJobTmpMetaStoreUrl(getProject(), mergeStep.getId()).toString(),
                mergeStep.getDistMetaUrl());

        CubeInstance cubeInstance = cubeMgr.reloadCube(CUBE_NAME);
        NSparkUpdateMetaAndCleanupAfterMergeStep cleanStep = job.getCleanUpAfterMergeStep();
        job.getParams().forEach((key, value) -> {
            Assert.assertEquals(value, mergeStep.getParam(key));
        });
        Assert.assertEquals(config.getJobTmpMetaStoreUrl(getProject(), cleanStep.getId()).toString(),
                cleanStep.getDistMetaUrl());
    }
}

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

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.stats.analyzer.TableAnalyzerJob;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

import lombok.val;
import lombok.var;

public class JobStepFactoryTest extends NLocalWithSparkSessionTest {
    private KylinConfig config;

    @Before
    public void setup() {
        config = getTestConfig();
    }

    @After
    public void after() {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
    }

    @Test
    public void testAddStepInSampling() {
        String table = "DEFAULT.TEST_KYLIN_FACT";
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, getProject());
        final TableDesc tableDesc = tableMetadataManager.getTableDesc(table);
        NTableSamplingJob job = NTableSamplingJob.create(tableDesc, getProject(), "ADMIN", 20000);
        Assert.assertEquals(table, job.getTargetSubject());
        Assert.assertEquals(getProject(), job.getParam(NBatchConstants.P_PROJECT_NAME));
        Assert.assertEquals(tableDesc.getIdentity(), job.getParam(NBatchConstants.P_TABLE_NAME));
        Assert.assertEquals("20000", job.getParam(NBatchConstants.P_SAMPLING_ROWS));
        Assert.assertEquals(JobTypeEnum.TABLE_SAMPLING, job.getJobType());

        final NResourceDetectStep resourceDetectStep = job.getResourceDetectStep();
        Assert.assertEquals(ResourceDetectBeforeSampling.class.getName(), resourceDetectStep.getSparkSubmitClassName());
        job.getParams().forEach((key, value) -> Assert.assertEquals(value, resourceDetectStep.getParam(key)));
        Assert.assertEquals(config.getJobTmpMetaStoreUrl(getProject(), resourceDetectStep.getId()).toString(),
                resourceDetectStep.getDistMetaUrl());

        final NTableSamplingJob.SamplingStep samplingStep = job.getSamplingStep();
        Assert.assertEquals(TableAnalyzerJob.class.getName(), samplingStep.getSparkSubmitClassName());
        job.getParams().forEach((key, value) -> Assert.assertEquals(value, samplingStep.getParam(key)));
        Assert.assertEquals(config.getJobTmpMetaStoreUrl(getProject(), samplingStep.getId()).toString(),
                samplingStep.getDistMetaUrl());
    }

    @Test
    public void testAddStepInSamplingFailedForTableNotExist() {
        final TableDesc tableDesc = NTableMetadataManager.getInstance(config, getProject()).getTableDesc("abc");
        try {
            NTableSamplingJob.create(tableDesc, getProject(), "ADMIN", 20000);
            Assert.fail();
        } catch (IllegalArgumentException ex) {
            Assert.assertEquals("Create table sampling job failed for table not exist!", ex.getMessage());
        }
    }

    @Test
    public void testAddStepInCubing() {
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        Set<NDataSegment> segments = Sets.newHashSet(oneSeg);
        Set<LayoutEntity> layouts = Sets.newHashSet(df.getIndexPlan().getAllLayouts());
        NSparkCubingJob job = NSparkCubingJob.create(segments, layouts, "ADMIN", null);
        Assert.assertEquals("89af4ee2-2cdb-4b07-b39e-4c29856309aa", job.getTargetSubject());

        NSparkExecutable resourceDetectStep = job.getResourceDetectStep();
        Assert.assertEquals(RDSegmentBuildJob.class.getName(), resourceDetectStep.getSparkSubmitClassName());
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

        SparkCleanupTransactionalTableStep cleanStep = job.getCleanIntermediateTableStep();
        Assert.assertNull(cleanStep);
    }

    @Test
    public void testAddStepInMerging() {
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflow flowCopy = dsMgr.getDataflow(df.getUuid()).copy();

        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        NDataSegment firstSeg = NDataSegment.empty();
        firstSeg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2010-01-02"),
                SegmentRange.dateToLong("2011-01-01")));
        firstSeg.setStatus(SegmentStatusEnum.READY);
        firstSeg.setId(RandomUtil.randomUUIDStr());

        NDataSegment secondSeg = NDataSegment.empty();
        secondSeg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2011-01-01"),
                SegmentRange.dateToLong("2013-01-01")));
        secondSeg.setStatus(SegmentStatusEnum.READY);
        secondSeg.setId(RandomUtil.randomUUIDStr());

        Segments<NDataSegment> mergingSegments = new Segments<>();
        mergingSegments.add(firstSeg);
        mergingSegments.add(secondSeg);
        flowCopy.setSegments(mergingSegments);

        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);

        NDataSegment mergedSegment = dsMgr.mergeSegments(flowCopy, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2010-01-02"), SegmentRange.dateToLong("2013-01-01")), true);
        Set<LayoutEntity> layouts = Sets.newHashSet(flowCopy.getIndexPlan().getAllLayouts());
        NSparkMergingJob job = NSparkMergingJob.merge(mergedSegment, Sets.newLinkedHashSet(layouts), "ADMIN",
                RandomUtil.randomUUIDStr());
        Assert.assertEquals("89af4ee2-2cdb-4b07-b39e-4c29856309aa", job.getTargetSubject());

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

        NSparkCleanupAfterMergeStep cleanStep = job.getCleanUpAfterMergeStep();
        job.getParams().forEach((key, value) -> {
            if (key.equalsIgnoreCase(NBatchConstants.P_SEGMENT_IDS)) {
                final Set<String> needDeleteSegmentIds = df.getMergingSegments(mergedSegment).stream()
                        .map(NDataSegment::getId).collect(Collectors.toSet());
                Assert.assertEquals(needDeleteSegmentIds, cleanStep.getSegmentIds());
            } else {
                Assert.assertEquals(value, mergeStep.getParam(key));
            }
        });
        Assert.assertEquals(config.getJobTmpMetaStoreUrl(getProject(), cleanStep.getId()).toString(),
                cleanStep.getDistMetaUrl());
    }

    private void cleanModel(String dataflowId) {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        var dataflow = dataflowManager.getDataflow(dataflowId);
        NDataflowUpdate update = new NDataflowUpdate(dataflow.getId());
        update.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
    }

    private AbstractExecutable mockJob(String jobId, long start, long end) {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        var dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataflow = dataflowManager.getDataflow(dataflow.getId());
        val layouts = dataflow.getIndexPlan().getAllLayouts();
        val oneSeg = dataflowManager.appendSegment(dataflow, new SegmentRange.TimePartitionedSegmentRange(start, end));
        NSparkCubingJob job = NSparkCubingJob.create(new JobFactory.JobBuildParams(Sets.newHashSet(oneSeg),
                Sets.newLinkedHashSet(layouts), "ADMIN", JobTypeEnum.INDEX_BUILD, jobId, null, null, null, null, null));
        NExecutableManager.getInstance(getTestConfig(), "default").addJob(job);
        return NExecutableManager.getInstance(getTestConfig(), "default").getJob(jobId);
    }
}

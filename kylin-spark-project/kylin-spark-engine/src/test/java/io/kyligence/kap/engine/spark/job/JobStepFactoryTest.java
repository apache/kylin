/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.job;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.spark_project.guava.collect.Sets;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.stats.analyzer.TableAnalyzerJob;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
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
        NSparkCubingJob job = NSparkCubingJob.create(segments, layouts, "ADMIN");
        Assert.assertEquals("89af4ee2-2cdb-4b07-b39e-4c29856309aa", job.getTargetSubject());

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
    public void testAddStepInMerging() {
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflow flowCopy = dsMgr.getDataflow(df.getUuid()).copy();

        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        NDataSegment firstSeg = new NDataSegment();
        firstSeg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2010-01-02"),
                SegmentRange.dateToLong("2011-01-01")));
        firstSeg.setStatus(SegmentStatusEnum.READY);
        firstSeg.setId(UUID.randomUUID().toString());

        NDataSegment secondSeg = new NDataSegment();
        secondSeg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2011-01-01"),
                SegmentRange.dateToLong("2013-01-01")));
        secondSeg.setStatus(SegmentStatusEnum.READY);
        secondSeg.setId(UUID.randomUUID().toString());

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
                UUID.randomUUID().toString());
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
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(layouts), "ADMIN",
                JobTypeEnum.INDEX_BUILD, jobId);
        NExecutableManager.getInstance(getTestConfig(), "default").addJob(job);
        return NExecutableManager.getInstance(getTestConfig(), "default").getJob(jobId);
    }
}

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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

import lombok.val;

public class ResourceDetectBeforeCubingJobTest extends NLocalWithSparkSessionTest {
    private KylinConfig config;

    @Override
    @Before
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();
        super.setUp();
        ss.sparkContext().setLogLevel("ERROR");

        config = getTestConfig();
        JobContextUtil.getJobContext(config);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        super.tearDown();
    }

    @Test
    public void testDoExecute() throws InterruptedException {
        val dfId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        cleanupSegments(dfId, getProject());
        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());

        Set<NDataSegment> segments = UnitOfWork.doInTransactionWithRetry(() -> {
            NDataflowManager dsMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
            NDataflow df = dsMgr.getDataflow(dfId);

            NDataSegment oneSeg = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                    SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("2012-02-01")));
            NDataSegment twoSeg = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                    SegmentRange.dateToLong("2012-02-01"), SegmentRange.dateToLong("2012-03-01")));
            return Sets.newHashSet(oneSeg, twoSeg);
        }, getProject());

        NDataflow df = NDataflowManager.getInstance(config, getProject()).getDataflow(dfId);
        Set<LayoutEntity> layouts = Sets.newHashSet(df.getIndexPlan().getAllLayouts());
        NSparkCubingJob job = NSparkCubingJob.create(segments, layouts, "ADMIN", null);
        Assert.assertEquals("89af4ee2-2cdb-4b07-b39e-4c29856309aa", job.getTargetSubject());

        NSparkExecutable resourceDetectStep = job.getResourceDetectStep();
        Assert.assertEquals(BeforeSegmentBuildJob.class.getName(), resourceDetectStep.getSparkSubmitClassName());

        // launch the job
        execMgr.addJob(job);

        // wait job done
        ExecutableState status = IndexDataConstructor.wait(job);
        Assert.assertEquals(IndexDataConstructor.firstFailedJobErrorMessage(execMgr, job), ExecutableState.SUCCEED,
                status);
    }

}

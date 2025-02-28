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
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

@Ignore("for test spark job on yarn")
public class NSparkCubingJobOnYarnTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        createTestMetadata();

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContext(getTestConfig());
    }

    @After
    public void after() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Test
    public void testSparkJobOnYarn() throws IOException, InterruptedException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.env.hdfs-working-dir", "hdfs://sandbox/kylin");
        config.setProperty("kylin.env", "DEV");
        config.setProperty("kylin.engine.spark.job-jar", "../assembly/target/ke-assembly-4.0.0-SNAPSHOT-job.jar");

        NDataflowManager dsMgr = NDataflowManager.getInstance(config, "default");
        ExecutableManager execMgr = ExecutableManager.getInstance(config, "default");

        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);

        NDataSegment oneSeg = dsMgr.appendSegment(df,
                new SegmentRange.TimePartitionedSegmentRange(0L, SegmentRange.dateToLong("2012-06-01")));
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();

        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(layouts), "ADMIN",
                null);

        // launch the job
        execMgr.addJob(job);

        ExecutableState status = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        oneSeg = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-06-01"), SegmentRange.dateToLong("2013-06-01")));

        job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(layouts), "ADMIN", null);

        // launch the job
        execMgr.addJob(job);

        // wait job done
        status = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        oneSeg = dsMgr.mergeSegments(df,
                new SegmentRange.TimePartitionedSegmentRange(0L, SegmentRange.dateToLong("2013-06-01")), false);

        NSparkMergingJob mergeJob = NSparkMergingJob.merge(oneSeg, Sets.newLinkedHashSet(layouts), "ADMIN",
                RandomUtil.randomUUIDStr());

        // launch the job
        execMgr.addJob(mergeJob);

        // wait job done
        status = wait(mergeJob);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

    }

    private ExecutableState wait(AbstractExecutable job) throws InterruptedException {
        while (true) {
            Thread.sleep(500);

            ExecutableState status = job.getStatus();
            if (!status.isProgressing()) {
                return status;
            }
        }
    }
}

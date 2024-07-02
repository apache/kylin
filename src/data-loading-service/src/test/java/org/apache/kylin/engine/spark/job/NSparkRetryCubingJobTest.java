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


import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTestBase;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.job.JobBucket;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class NSparkRetryCubingJobTest extends NLocalWithSparkSessionTestBase {
    private KylinConfig config;

    @Override
    @Before
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();
        super.setUp();
        config = getTestConfig();
        createTestMetadata();
        JobContextUtil.getJobContext(getTestConfig());
        populateSSWithCSVData(getTestConfig(), getProject(), ss);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        super.tearDown();
    }

    public String getProject() {
        return "TestBuildV2Dict";
    }

    @Test
    public void testRetryIndexBuildJob() throws InterruptedException, SQLException {
        config.setProperty("kylin.env", "DEV");
        config.setProperty("kylin.engine.spark.job-jar", "../assembly/target/kylin-assembly-5.0.0-SNAPSHOT-job.jar");
        config.setProperty("kylin.engine.spark.build-class-name", "org.apache.kylin.engine.spark.MockSegmentBuildJobWithRetry");
        val prj = getProject();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, prj);
        ExecutableManager execMgr = ExecutableManager.getInstance(config, prj);
        val indexDataConstructor = new IndexDataConstructor(prj);

        val preDfID = "493b5213-18e4-29f2-c801-d56c4617ef4f";
        NDataflow preDF = dsMgr.getDataflow(preDfID);
        NDataSegment preSegment = preDF.getFirstSegment();
        Set<LayoutEntity> toPreBuildLayouts = new HashSet<>();
        toPreBuildLayouts.add(preDF.getIndexPlan().getLayoutEntity(1L));
        toPreBuildLayouts.add(preDF.getIndexPlan().getLayoutEntity(20000000001L));
        indexDataConstructor.buildSegment(preDfID, preSegment, toPreBuildLayouts, false, null);

        val dfID = "5e67c064-46d1-7893-7620-95d27f8cacee";
        NDataflow df = dsMgr.getDataflow(dfID);
        NDataSegment segment = df.getFirstSegment();
        // build new added index 30001 and 20000010001
        Set<LayoutEntity> toBuildLayouts = new HashSet<>();
        toBuildLayouts.add(df.getIndexPlan().getLayoutEntity(30001L));
        toBuildLayouts.add(df.getIndexPlan().getLayoutEntity(20000010001L));
        Set<JobBucket> buckets = Sets.newHashSet();
        indexDataConstructor.buildSegment(dfID, segment, toBuildLayouts, false, null);
        QueryExec queryExec = new QueryExec(prj, NProjectManager.getProjectConfig(prj), true);
        val sql = "select count(distinct LO_ORDERPRIOTITY) from ssb.lineorder";
        queryExec.executeQuery(sql);
        val result = SparderEnv.getDF().collectAsList().get(0).getLong(0);
        long expect = ss.sql("select count(distinct LO_ORDERPRIOTITY) from lineorder").collectAsList().get(0).getLong(0);
        Assert.assertEquals(expect, result);

    }
}

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
package org.apache.kylin.streaming.jobs;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.streaming.util.StreamingTestCase;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;

public class SyncMergerTest extends StreamingTestCase {

    private static String PROJECT = "streaming_test";
    private static String MODEL_ALIAS = "stream_merge1";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testRunSuccessful() {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflowByModelAlias(MODEL_ALIAS);
        val ss = SparkSession.builder().master("local").getOrCreate();
        val mergeJobEntry = createMergeJobEntry(mgr, df, ss, PROJECT);
        val afterMergeSeg = mergeJobEntry.afterMergeSegment();
        val syncMerge = new SyncMerger(mergeJobEntry);
        val merger = new StreamingDFMergeJob();
        syncMerge.run(merger);
        df = mgr.getDataflow(df.getId());
        Assert.assertEquals(1, df.getSegments().size());
        Assert.assertEquals(SegmentStatusEnum.READY, df.getSegment(afterMergeSeg.getId()).getStatus());
        ss.stop();
    }

    @Test
    public void testRunFailed() {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflowByModelAlias(MODEL_ALIAS);
        val mergeJobEntry = createMergeJobEntry(mgr, df, null, PROJECT);
        val syncMerge = new SyncMerger(mergeJobEntry);
        val merger = new StreamingDFMergeJob();
        try {
            syncMerge.run(merger);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
        }
    }

}

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
package org.apache.kylin.streaming.merge;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.streaming.util.StreamingTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;
import lombok.var;

public class CatchupMergePolicyTest extends StreamingTestCase {

    private static String PROJECT = "streaming_test";
    private static String MODEL_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";
    private static String DATAFLOW_ID = MODEL_ID;
    private static int thresholdOf1k = 1024;
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private NDataflowManager mgr;
    private CatchupMergePolicy catchupMergePolicy;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        catchupMergePolicy = new CatchupMergePolicy();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    /**
     * test no matched seg list
     */
    @Test
    public void testSelectMatchedSegList1() {
        val dataflow = mgr.getDataflow(DATAFLOW_ID);
        val segments = dataflow.getSegments().getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        Assert.assertTrue(segments.get(0).getStorageBytesSize() > thresholdOf1k);
        Assert.assertTrue(!segments.get(0).getAdditionalInfo().isEmpty());
        Assert.assertTrue(segments.get(1).getStorageBytesSize() > thresholdOf1k);
        Assert.assertTrue(!segments.get(1).getAdditionalInfo().isEmpty());
        Assert.assertTrue(segments.get(2).getStorageBytesSize() > thresholdOf1k);
        Assert.assertTrue(segments.get(2).getAdditionalInfo().isEmpty());

        var matchedSegList = catchupMergePolicy.selectMatchedSegList(segments, 0, thresholdOf1k, 3);
        Assert.assertEquals(0, matchedSegList.size());

        var matchedSegList1 = catchupMergePolicy.selectMatchedSegList(segments, 0, thresholdOf1k * 8, 3);
        Assert.assertEquals(0, matchedSegList1.size());
    }

    /**
     * L0 layer catchup
     */
    @Test
    public void testSelectMatchedSegListOfLayer0() throws IOException {
        KylinConfig testConfig = getTestConfig();
        val copy = createIndexPlan(testConfig, PROJECT, MODEL_ID, MODEL_ALIAS);
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.createDataflow(copy, "test_owner");

        df = createSegments(mgr, df, 30);
        df = setSegmentStorageSize(mgr, df, 1024L);
        val segments = df.getSegments();
        var matchedSegList = catchupMergePolicy.selectMatchedSegList(segments, 0, thresholdOf1k * 20, 3);
        // 30: 20K/1K * 1.5
        Assert.assertEquals(30, matchedSegList.size());

        var matchedSegList1 = catchupMergePolicy.selectMatchedSegList(segments, 0, thresholdOf1k * 3, 3);
        // 5: 3K/1K * 1.5
        Assert.assertEquals(5, matchedSegList1.size());
    }

    /**
     * L1 layer catchup
     */
    @Test
    public void testSelectMatchedSegListOfLayer1() throws IOException {
        KylinConfig testConfig = getTestConfig();
        val copy = createIndexPlan(testConfig, PROJECT, MODEL_ID, MODEL_ALIAS);
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.createDataflow(copy, "test_owner");

        df = createSegments(mgr, df, 30, 1);
        df = setSegmentStorageSize(mgr, df, 1024L);
        val segments = df.getSegments();
        var matchedSegList = catchupMergePolicy.selectMatchedSegList(segments, 1, thresholdOf1k * 20, 3);
        // 30: 20K/1K * 1.5
        Assert.assertEquals(30, matchedSegList.size());

        var matchedSegList1 = catchupMergePolicy.selectMatchedSegList(segments, 1, thresholdOf1k * 3, 3);
        // 5: 3K/1K * 1.5
        Assert.assertEquals(5, matchedSegList1.size());
    }

    /**
     * L1 & L0 layer catchup
     */
    @Test
    public void testSelectMatchedSegList4() throws IOException {
        KylinConfig testConfig = getTestConfig();
        val copy = createIndexPlan(testConfig, PROJECT, MODEL_ID, MODEL_ALIAS);
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.createDataflow(copy, "test_owner");

        df = createSegments(mgr, df, 30, null, copyForWrite -> {
            for (int i = 0; i < 5; i++) {
                val seg = copyForWrite.getSegments().get(i);
                seg.getAdditionalInfo().put("file_layer", "1");
            }
        });
        df = setSegmentStorageSize(mgr, df, 1024L);

        val segments = df.getSegments();
        var matchedSegList = catchupMergePolicy.selectMatchedSegList(segments, 0, thresholdOf1k * 30, 3);
        Assert.assertEquals(30, matchedSegList.size());

        var matchedSegList1 = catchupMergePolicy.selectMatchedSegList(segments, 0, thresholdOf1k * 3, 3);
        Assert.assertEquals(5, matchedSegList1.size());
        matchedSegList1.stream().forEach(item -> {
            Assert.assertEquals("1", item.getAdditionalInfo().get("file_layer"));
        });
    }
}

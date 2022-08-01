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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.streaming.util.ReflectionUtils;
import org.apache.kylin.streaming.util.StreamingTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;
import lombok.var;

public class PeakMergePolicyTest extends StreamingTestCase {

    private static String PROJECT = "streaming_test";
    private static String DATAFLOW_ID = "e78a89dd-847f-4574-8afa-8768b4228b73_rt";
    private static String MODEL_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private NDataflowManager mgr;
    private PeakMergePolicy peakMergePolicy;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        peakMergePolicy = new PeakMergePolicy();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    /**
     * test no matched seg list
     */
    public void testSelectMatchedSegList1() {
        val thresholdOf20k = StreamingUtils.parseSize("20k");
        KylinConfig testConfig = getTestConfig();
        val copy = createIndexPlan(testConfig, PROJECT, MODEL_ID, MODEL_ALIAS);
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.createDataflow(copy, "test_owner");

        df = createSegments(mgr, df, 3);
        for (int i = 0; i < df.getSegments().size(); i++) {
            ReflectionUtils.setField(df.getSegments().get(i), "storageSize", 2048L);
        }
        df = mgr.getDataflow(df.getId());
        val segments = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);

        var matchedSegList = peakMergePolicy.selectMatchedSegList(segments, 0, thresholdOf20k, 3);
        Assert.assertEquals(0, matchedSegList.size());
    }

    @Test
    public void testSelectMatchedSegList() {
        val thresholdOf4k = StreamingUtils.parseSize("4k");
        KylinConfig testConfig = getTestConfig();
        val copy = createIndexPlan(testConfig, PROJECT, MODEL_ID, MODEL_ALIAS);
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.createDataflow(copy, "test_owner");

        df = createSegments(mgr, df, 3);
        for (int i = 0; i < df.getSegments().size(); i++) {
            if (i < 2) {
                ReflectionUtils.setField(df.getSegments().get(i), "storageSize", 2048L);
            } else {
                ReflectionUtils.setField(df.getSegments().get(i), "storageSize", 4096L);
            }
        }
        df = mgr.getDataflow(df.getId());
        val segments = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        var matchedSegList = peakMergePolicy.selectMatchedSegList(segments, 0, thresholdOf4k, 3);
        Assert.assertEquals(3, matchedSegList.size());

        matchedSegList = peakMergePolicy.selectMatchedSegList(segments, 0, thresholdOf4k, 5);
        Assert.assertEquals(3, matchedSegList.size());

        matchedSegList = peakMergePolicy.selectMatchedSegList(segments, 0, StreamingUtils.parseSize("20k"), 3);
        Assert.assertEquals(0, matchedSegList.size());

        matchedSegList = peakMergePolicy.selectMatchedSegList(segments, 0, thresholdOf4k, 2);
        Assert.assertEquals(0, matchedSegList.size());
    }

}

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

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.streaming.util.ReflectionUtils;
import org.apache.kylin.streaming.util.StreamingTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;

public class MergePolicyTest extends StreamingTestCase {

    private static String PROJECT = "streaming_test";
    private static String MODEL_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private NDataflowManager mgr;
    private MergePolicy mergePolicy;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        mergePolicy = new MergePolicy() {
            public int findStartIndex(List<NDataSegment> segList, Long thresholdOfSegSize) {
                return super.findStartIndex(segList, thresholdOfSegSize);
            }

            @Override
            public List<NDataSegment> selectMatchedSegList(List<NDataSegment> segList, int layer,
                    long thresholdOfSegSize, int numOfSeg) {
                return null;
            }

            @Override
            public boolean matchMergeCondition(long thresholdOfSegSize) {
                return false;
            }
        };
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testFindStartIndex() {
        KylinConfig testConfig = getTestConfig();
        val copy = createIndexPlan(testConfig, PROJECT, MODEL_ID, MODEL_ALIAS);
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.createDataflow(copy, "test_owner");

        df = createSegments(mgr, df, 30);
        for (int i = 0; i < df.getSegments().size(); i++) {
            if (i < 10) {
                ReflectionUtils.setField(df.getSegments().get(i), "storageSize", 4096L);
            } else {
                ReflectionUtils.setField(df.getSegments().get(i), "storageSize", 2048L);
            }
        }
        df = mgr.getDataflow(df.getId());
        val segments = df.getSegments();

        val startIndex = mergePolicy.findStartIndex(segments, 1024L);
        Assert.assertEquals(-1, startIndex);

        val startIndex1 = mergePolicy.findStartIndex(segments, 20480L);
        Assert.assertEquals(0, startIndex1);

        val startIndex2 = mergePolicy.findStartIndex(segments, 2048L);
        Assert.assertEquals(10, startIndex2);
    }

    @Test
    public void testIsThresholdOfSegSizeOver() {
        val thresholdOver = mergePolicy.isThresholdOfSegSizeOver(20480, 10240);
        Assert.assertEquals(true, thresholdOver);

        val thresholdOver1 = mergePolicy.isThresholdOfSegSizeOver(10240, 10240);
        Assert.assertEquals(false, thresholdOver1);
    }
}

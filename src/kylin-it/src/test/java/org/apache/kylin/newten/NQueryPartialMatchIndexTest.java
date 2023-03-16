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


package org.apache.kylin.newten;

import java.util.List;

import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.util.ExecAndComp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.val;

public class NQueryPartialMatchIndexTest extends NLocalWithSparkSessionTest {

    private String dfName = "cce7b90d-c1ac-49ef-abc3-f8971eb91544";

    @Before
    public void setup() throws Exception {
        this.createTestMetadata("src/test/resources/ut_meta/partial_match_index");
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Override
    public String getProject() {
        return "kylin";
    }

    @Test
    public void testQueryPartialMatchIndex() throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), "kylin");
        NDataflow df = dsMgr.getDataflow(dfName);
        String sql = "select count(*) from TEST_KYLIN_FACT where cal_dt > '2012-01-01' and cal_dt < '2014-01-01'";

        val expectedRanges = Lists.<Pair<String, String>>newArrayList();
        val segmentRange = Pair.newPair("2012-01-01", "2013-01-01");
        expectedRanges.add(segmentRange);

        QueryContext.current().setPartialMatchIndex(true);
        ExecAndComp.queryModelWithoutCompute(getProject(), sql);
        val context = ContextUtil.listContexts().get(0);
        val segmentIds = context.storageContext.getPrunedSegments();
        assertPrunedSegmentRange(df.getModel().getId(), segmentIds, expectedRanges);
    }

    @Test
    public void testQueryPartialMatchIndexWhenPushdown() throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), "kylin");

        NDataflow df = dsMgr.getDataflow(dfName);
        String sql = "select count(*) from TEST_KYLIN_FACT where cal_dt > '2013-01-01' and cal_dt < '2014-01-01'";

        QueryContext.current().setPartialMatchIndex(true);
        try {
            ExecAndComp.queryModelWithoutCompute(getProject(), sql);
        } catch (Exception e) {
            if (e.getCause() instanceof NoRealizationFoundException) {
                val context = ContextUtil.listContexts().get(0);
                val segmentIds = context.storageContext.getPrunedSegments();
                assertPrunedSegmentRange(df.getModel().getId(), segmentIds, Lists.newArrayList());
            }
        }
    }


    private void assertPrunedSegmentRange(String dfId, List<NDataSegment> prunedSegments,
                                          List<Pair<String, String>> expectedRanges) {
        val model = NDataModelManager.getInstance(getTestConfig(), getProject()).getDataModelDesc(dfId);
        val partitionColDateFormat = model.getPartitionDesc().getPartitionDateFormat();

        if (org.apache.commons.collections.CollectionUtils.isEmpty(expectedRanges)) {
            return;
        }
        Assert.assertEquals(expectedRanges.size(), prunedSegments.size());
        for (int i = 0; i < prunedSegments.size(); i++) {
            val segment = prunedSegments.get(i);
            val start = DateFormat.formatToDateStr(segment.getTSRange().getStart(), partitionColDateFormat);
            val end = DateFormat.formatToDateStr(segment.getTSRange().getEnd(), partitionColDateFormat);
            val expectedRange = expectedRanges.get(i);
            Assert.assertEquals(expectedRange.getFirst(), start);
            Assert.assertEquals(expectedRange.getSecond(), end);
        }
    }
}

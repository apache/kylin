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
import java.util.stream.Collectors;

import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

import lombok.val;

public class NMultipleColumnsInTest extends NLocalWithSparkSessionTest {
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.createTestMetadata("src/test/resources/ut_meta/multiple_columns_in");

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContext(getTestConfig());
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/multiple_columns_in" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Override
    public String getProject() {
        return "multiple_columns_in";
    }

    @Test
    public void test() throws Exception {
        val dfName = "7c670664-8d05-466a-802f-83c023b56c77";
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        val layouts = df.getIndexPlan().getAllLayouts();
        indexDataConstructor.buildIndex(dfName, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.newLinkedHashSet(layouts), true);

        overwriteSystemProp("calcite.keep-in-clause", "true");
        overwriteSystemProp("calcite.convert-multiple-columns-in-to-or", "true");
        runCase();

        overwriteSystemProp("calcite.keep-in-clause", "false");
        runCase();
    }

    private void runCase() throws Exception {
        // test use multiple_columns_in with other filter
        String actual_sql1 = "select count(*) as val, IS_EFFECTUAL, LSTG_FORMAT_NAME from TEST_KYLIN_FACT "
                + "where (IS_EFFECTUAL, LSTG_FORMAT_NAME) in ((false, 'FP-GTC'), (true, 'Auction')) and IS_EFFECTUAL in (false)"
                + "group by IS_EFFECTUAL, LSTG_FORMAT_NAME order by val";
        String expect_sql1 = "select count(*) as val, IS_EFFECTUAL, LSTG_FORMAT_NAME from TEST_KYLIN_FACT "
                + "where ((IS_EFFECTUAL = false and LSTG_FORMAT_NAME = 'FP-GTC') or (IS_EFFECTUAL = true and LSTG_FORMAT_NAME = 'Auction')) and IS_EFFECTUAL in (false)"
                + "group by IS_EFFECTUAL, LSTG_FORMAT_NAME order by val";
        assertSameResults(actual_sql1, expect_sql1);

        // test use multiple_columns_in alone
        String actual_sql2 = "select count(*) as val, IS_EFFECTUAL, LSTG_FORMAT_NAME from TEST_KYLIN_FACT "
                + "where (IS_EFFECTUAL, LSTG_FORMAT_NAME) in ((false, 'FP-GTC'), (true, 'Auction'))"
                + "group by IS_EFFECTUAL, LSTG_FORMAT_NAME order by val";

        String expect_sql2 = "select count(*) as val, IS_EFFECTUAL, LSTG_FORMAT_NAME from TEST_KYLIN_FACT "
                + "where ((IS_EFFECTUAL = false and LSTG_FORMAT_NAME = 'FP-GTC') or (IS_EFFECTUAL = true and LSTG_FORMAT_NAME = 'Auction'))"
                + "group by IS_EFFECTUAL, LSTG_FORMAT_NAME order by val";
        assertSameResults(actual_sql2, expect_sql2);
    }

    private void assertSameResults(String actualSql, String expectSql) throws Exception {
        List<String> actualResults = ExecAndComp.queryModelWithoutCompute(getProject(), actualSql).collectAsList()
                .stream().map(Row::toString).collect(Collectors.toList());
        List<String> expectResults = ExecAndComp.queryModelWithoutCompute(getProject(), expectSql).collectAsList()
                .stream().map(Row::toString).collect(Collectors.toList());
        Assert.assertTrue(actualResults.containsAll(expectResults) && expectResults.containsAll(actualResults));
    }
}

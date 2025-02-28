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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.common.SparderQueryTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

public class NFlattableJoinWithoutLookupTest extends NLocalWithSparkSessionTest {

    private NDataflowManager dfMgr = null;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        overwriteSystemProp("kylin.job.flat-table-join-without-lookup", "true");
        overwriteSystemProp("kylin.engine.persist-flattable-enabled", "false");
        this.createTestMetadata("src/test/resources/ut_meta/flattable_without_join_lookup");
        dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContext(getTestConfig());
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/flattable_without_join_lookup" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
        FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
    }

    @Override
    public String getProject() {
        return "flattable_without_join_lookup";
    }

    private String sql = "select  CAL_DT as dt1, cast (TEST_ORDER_STRING.TEST_TIME_ENC as timestamp) as ts2, cast(TEST_ORDER_STRING.TEST_DATE_ENC  as date) as dt2,TEST_ORDER.ORDER_ID, count(*) FROM TEST_ORDER LEFT JOIN TEST_KYLIN_FACT ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID LEFT JOIN TEST_ORDER_STRING on TEST_ORDER.ORDER_ID = TEST_ORDER_STRING.ORDER_ID group by TEST_ORDER.ORDER_ID ,TEST_ORDER_STRING.TEST_TIME_ENC , TEST_ORDER_STRING.TEST_DATE_ENC ,CAL_DT order by TEST_ORDER.ORDER_ID,TEST_ORDER_STRING.TEST_TIME_ENC , TEST_ORDER_STRING.TEST_DATE_ENC ,CAL_DT ";

    @Test
    public void testFlattableWithoutLookup() throws Exception {
        buildSegs("8c670664-8d05-466a-802f-83c023b56c77", 10001L);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        Dataset<Row> cube = ExecAndComp.queryModel(getProject(), sql);
        Dataset<Row> pushDown = ExecAndComp.querySparkSql(sql);
        String msg = SparderQueryTest.checkAnswer(cube, pushDown, true);
        Assert.assertNull(msg);
    }

    @Test
    public void testFlattableJoinLookup() throws Exception {
        buildSegs("9cde9d25-9334-4b92-b229-a00f49453757", 10001L);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        Dataset<Row> cube = ExecAndComp.queryModel(getProject(), sql);
        Dataset<Row> pushDown = ExecAndComp.querySparkSql(sql);
        String msg = SparderQueryTest.checkAnswer(cube, pushDown, true);
        Assert.assertNull(msg);
    }

    private void buildSegs(String dfName, long... layoutID) throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = new ArrayList<>();
        IndexPlan indexPlan = df.getIndexPlan();
        if (layoutID.length == 0) {
            layouts = indexPlan.getAllLayouts();
        } else {
            for (long id : layoutID) {
                layouts.add(indexPlan.getLayoutEntity(id));
            }
        }
        long start = SegmentRange.dateToLong("2009-01-01 00:00:00");
        long end = SegmentRange.dateToLong("2015-01-01 00:00:00");
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);
    }
}

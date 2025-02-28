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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.util.ExecAndComp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

public class ReuseFlatTableTest extends NLocalWithSparkSessionTest {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        overwriteSystemProp("kylin.engine.persist-flattable-enabled", "true");
        setOverlay("src/test/resources/ut_meta/reuse_flattable");

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContext(getTestConfig());

        populateSSWithCSVData(getTestConfig(), getProject(), ss);
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/reuse_flattable" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        super.tearDown();
    }

    @Override
    public String getProject() {
        return "reuse_flattable";
    }

    @Test
    public void testReuseFlatTable() throws Exception {
        String dfID = "75080248-367e-4bac-9fd7-322517ee0227";
        fullBuild(dfID);
        NDataflowManager dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        NDataflow dataflow = dfManager.getDataflow(dfID);
        NDataSegment firstSegment = dataflow.getFirstSegment();
        UnitOfWork.doInTransactionWithRetry(() -> {
            NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(),
                    getProject());
            indexPlanManager.updateIndexPlan(dfID, copyForWrite -> {
                IndexEntity indexEntity = new IndexEntity();
                indexEntity.setId(200000);
                indexEntity.setDimensions(Lists.newArrayList(2));
                indexEntity.setMeasures(Lists.newArrayList(100000, 100001));
                LayoutEntity layout = new LayoutEntity();
                layout.setId(200001);
                layout.setColOrder(Lists.newArrayList(2, 100000, 100001));
                layout.setIndex(indexEntity);
                layout.setAuto(true);
                layout.setUpdateTime(0);
                indexEntity.setLayouts(Lists.newArrayList(layout));
                copyForWrite.setIndexes(Lists.newArrayList(indexEntity));
            });
            return null;
        }, getProject());

        indexDataConstructor.buildSegment(dfID, firstSegment,
                Sets.newLinkedHashSet(dfManager.getDataflow(dfID).getIndexPlan().getAllLayouts()), true, null);
        String query = "select count(distinct trans_id) from TEST_KYLIN_FACT";
        long result = ExecAndComp.queryModel(getProject(), query).collectAsList().get(0).getLong(0);
        long expect = ss.sql("select count(distinct trans_id) from TEST_KYLIN_FACT").collectAsList().get(0).getLong(0);
        Assert.assertEquals(result, expect);
    }
}

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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

import lombok.val;

public class NComputedColumnTest extends NLocalWithSparkSessionTest {
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        JobContextUtil.cleanUp();
        this.createTestMetadata("src/test/resources/ut_meta/comput_column");

        JobContextUtil.getJobContext(getTestConfig());
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/comput_column" };
    }

    @Override
    @After
    public void tearDown() {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Override
    public String getProject() {
        return "comput_column";
    }

    @Test
    public void testConstantComputeColumn() throws Exception {
        String dfID = "4a45dc4d-937e-43cc-8faa-34d59d4e11d3";
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfID);
        val layouts = df.getIndexPlan().getAllLayouts();
        indexDataConstructor.buildIndex(dfID, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.newLinkedHashSet(layouts), true);
        String sqlHitCube = "select (1+2) as c1,(LINEORDER.LO_TAX +1) as c2,(CUSTOMER.C_NAME +'USA') as c3 "
                + "from SSB.P_LINEORDER as LINEORDER join SSB.CUSTOMER as CUSTOMER on LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY "
                + "group by (1+2),(LINEORDER.LO_TAX +1),(CUSTOMER.C_NAME +'USA') ";
        List<String> hitCubeResult = ExecAndComp.queryModelWithoutCompute(getProject(), sqlHitCube).collectAsList()
                .stream().map(Row::toString).collect(Collectors.toList());
        Assert.assertEquals(9, hitCubeResult.size());
    }

    @Test
    public void testCCNamedEqualsDimensionName() throws Exception {
        String modelId = "4a45dc4d-937e-43cc-8faa-34d59d4e11d3";
        String FACT_TABLE = "SSB.P_LINEORDER";
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        modelManager.updateDataModel(modelId, copyForWrite -> {
            ComputedColumnDesc cc1 = new ComputedColumnDesc();
            cc1.setTableAlias("P_LINEORDER");
            cc1.setTableIdentity(FACT_TABLE);
            cc1.setComment("");
            cc1.setColumnName("C_NAME");
            cc1.setDatatype("varchar");
            cc1.setExpression("CUSTOMER.c_NAME");
            cc1.setInnerExpression("CUSTOMER.c_NAME");
            copyForWrite.getComputedColumnDescs().add(cc1);

            NDataModel.NamedColumn column1 = new NDataModel.NamedColumn();
            column1.setName("c_NAME");
            column1.setId(copyForWrite.getAllNamedColumns().size());
            column1.setAliasDotColumn("P_LINEORDER.c_NAME");
            column1.setStatus(NDataModel.ColumnStatus.DIMENSION);
            copyForWrite.getAllNamedColumns().add(column1);
        });

        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "true");

        try {
            ExecAndComp.queryModelWithoutCompute(getProject(),
                    "select C_NAME from SSB.P_LINEORDER as LINEORDER join SSB.CUSTOMER as CUSTOMER on LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY limit 500");
        } catch (Exception | StackOverflowError e) {
            Assert.assertFalse(e instanceof StackOverflowError);
        }
    }
}

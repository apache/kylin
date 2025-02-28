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

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.util.ExecAndComp;
import org.apache.kylin.util.ExecAndComp.CompareLevel;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class NMatchingTest extends NLocalWithSparkSessionTest {

    @Override
    @Before
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();
        super.setUp();
        overwriteSystemProp("kylin.engine.persist-flattable-enabled", "false");
        JobContextUtil.getJobContext(getTestConfig());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Override
    public String getProject() {
        return "match";
    }

    @Test
    public void testCanNotAnswer() throws Exception {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        dfMgr.updateDataflowStatus("073198da-ce0e-4a0c-af38-cc27ae31cc0e", RealizationStatusEnum.OFFLINE);
        fullBuild("83ade475-5b80-483a-ae4b-1144e4f04e81");

        try {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());

            List<Pair<String, String>> query = new ArrayList<>();
            query.add(
                    Pair.newPair("can_not_answer", "select sum(price) from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
            ExecAndComp.execAndCompare(query, getProject(), CompareLevel.SAME, "left");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(Throwables.getRootCause(e).getMessage().contains("No realization found for OlapContext"));
        }
    }

    @Test
    public void testCanAnswer() throws Exception {
        ss.sparkContext().setLogLevel("ERROR");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        fullBuild("83ade475-5b80-483a-ae4b-1144e4f04e81");
        fullBuild("073198da-ce0e-4a0c-af38-cc27ae31cc0e");

        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("can_not_answer", "select sum(price) from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        ExecAndComp.execAndCompare(query, getProject(), CompareLevel.SAME, "left");
    }
}

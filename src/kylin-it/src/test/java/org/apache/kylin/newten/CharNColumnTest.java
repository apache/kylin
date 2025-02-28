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
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CharNColumnTest extends NLocalWithSparkSessionTest {

    @Override
    @Before
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();
        setOverlay("src/test/resources/ut_meta/test_char_n_column");
        super.setUp();
        overwriteSystemProp("kylin.engine.persist-flattable-enabled", "false");
        JobContextUtil.getJobContext(getTestConfig());
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/test_char_n_column" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        super.tearDown();
    }

    @Override
    public String getProject() {
        return "char_n_column";
    }

    @Test
    public void testCharNColumn() throws Exception {
        fullBuild("c9ddd37e-c870-4ccf-a131-5eef8fe6cb7e");

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());
        String query1 = "select AGE, CITY, " + "intersect_count(USER_ID, TAG, array['rich','tall','handsome']) "
                + "from TEST_CHAR_N where city='Beijing  ' group by AGE, CITY ";
        List<String> r1 = ExecAndComp.queryModel(getProject(), query1).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());

        Assert.assertEquals("19,Beijing   ,0", r1.get(0));

    }
}

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

package org.apache.kylin.gluten;

import org.apache.gluten.execution.FilterExecTransformer;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.util.SuggestTestBase;
import org.apache.spark.sql.common.GlutenTestConfig;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class GlutenKEConfigTest extends SuggestTestBase {

    private static final String PROJECT = "newten";

    @Test
    public void testIndexNotUseGluten() throws Exception {
        overwriteSystemProp("kylin.query.index-use-gulten", "false");
        String sql = "select count(*) from test_kylin_fact";
        proposeAndBuildIndex(new String[] { sql });
        QueryExec queryExec = new QueryExec(PROJECT, KylinConfig.getInstanceFromEnv());
        val resultSet = queryExec.executeQuery(sql);
        Assert.assertTrue(resultSet.getSize() > 0);
    }

    @Test
    public void testGlutenIsEnable() throws Exception {
        if (!GlutenTestConfig.enableGluten()) {
            return;
        }
        val a = ss.range(100).filter("id < 5").queryExecution().executedPlan();
        Assert.assertTrue(a.find(FilterExecTransformer.class::isInstance).nonEmpty());
    }

    private void proposeAndBuildIndex(String[] sqls) throws InterruptedException {
        AccelerationUtil.runWithSmartContext(KylinConfig.getInstanceFromEnv(), PROJECT, sqls, true);
        buildAllModels(KylinConfig.getInstanceFromEnv(), PROJECT);
    }

}

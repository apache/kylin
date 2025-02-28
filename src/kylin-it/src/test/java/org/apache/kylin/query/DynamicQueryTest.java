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

package org.apache.kylin.query;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.util.SuggestTestBase;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class DynamicQueryTest extends SuggestTestBase {
    private static final String PROJECT = "newten";

    @Test
    public void testDynamicParamOnAgg() throws Exception {
        proposeAndBuildIndex(new String[] { "select * from test_kylin_fact" });
        getTestConfig().setProperty("kylin.query.use-tableindex-answer-non-raw-query", "true");

        String sql = "select max(LSTG_SITE_ID/?) from TEST_KYLIN_FACT";
        QueryExec queryExec = new QueryExec(PROJECT, KylinConfig.getInstanceFromEnv());
        queryExec.setPrepareParam(0, 2);
        val resultSet = queryExec.executeQuery(sql);
        Assert.assertFalse(resultSet.getRows().isEmpty());
        Assert.assertEquals("105.5", resultSet.getRows().get(0).get(0));
    }

    @Test
    public void testDynamicParamOnLimitOffset() throws Exception {
        proposeAndBuildIndex(new String[] {
                "select * from (select cal_dt, count(*) from test_kylin_fact group by cal_dt order by cal_dt) as test_kylin_fact order by cal_dt" });

        String sql = "select * from (select cal_dt, count(*) from test_kylin_fact group by cal_dt order by cal_dt limit ? offset ?) as test_kylin_fact order by cal_dt limit ? offset ?";
        QueryExec queryExec = new QueryExec(PROJECT, KylinConfig.getInstanceFromEnv());
        queryExec.setPrepareParam(0, 100);
        queryExec.setPrepareParam(1, 100);
        queryExec.setPrepareParam(2, 1);
        queryExec.setPrepareParam(3, 10);
        val resultSet = queryExec.executeQuery(sql);
        Assert.assertEquals(1, resultSet.getRows().size());
        val date = resultSet.getRows().get(0).get(0);
        val count = resultSet.getRows().get(0).get(1);
        Assert.assertEquals("2012-04-21", date);
        Assert.assertEquals("16", count);
    }

    private void proposeAndBuildIndex(String[] sqls) throws InterruptedException {
        AccelerationUtil.runWithSmartContext(KylinConfig.getInstanceFromEnv(), PROJECT, sqls, true);
        buildAllModels(KylinConfig.getInstanceFromEnv(), PROJECT);
    }

    @Test
    public void testDynamicParamOnMilliSecTimestamp() throws Exception {
        proposeAndBuildIndex(new String[] { "select time2 from test_measure" });
        String sql = "select time2 from test_measure where time2=?";
        QueryExec queryExec = new QueryExec(PROJECT, KylinConfig.getInstanceFromEnv());
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS",
                Locale.getDefault(Locale.Category.FORMAT));
        String ts = "2012-03-21 10:10:10.789";
        Date parsedDate = dateFormat.parse(ts);
        queryExec.setPrepareParam(0, new java.sql.Timestamp(parsedDate.getTime()));
        val resultSet = queryExec.executeQuery(sql);
        Assert.assertEquals(1, resultSet.getRows().size());
        Assert.assertEquals("2012-03-21 10:10:10.789", resultSet.getRows().get(0).get(0));
    }
}

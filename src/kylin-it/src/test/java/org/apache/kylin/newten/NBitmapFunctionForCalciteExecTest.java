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
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.query.engine.QueryRoutingEngine;
import org.apache.kylin.query.engine.data.QueryResult;
import org.apache.kylin.query.util.QueryParams;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NBitmapFunctionForCalciteExecTest extends NLocalWithSparkSessionTest {

    private Logger logger = LoggerFactory.getLogger(NBitmapFunctionForCalciteExecTest.class);

    @Mock
    private QueryRoutingEngine queryRoutingEngine = Mockito.spy(QueryRoutingEngine.class);

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        populateSSWithCSVData(getTestConfig(), getProject(), ss);
        Unsafe.setProperty("kylin.query.engine.run-constant-query-locally", "true");
    }

    @Override
    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
        FileUtils.deleteQuietly(new File("../kylin-it/metastore_db"));
        Unsafe.clearProperty("kylin.query.engine.run-constant-query-locally");
    }

    @Override
    public String getProject() {
        return "intersect_count";
    }

    @Test
    public void testIntersectCountForFalseFilter() throws Exception {
        logger.info("comming....");
        String query = "select "
                + "intersect_count_v2(TEST_COUNT_DISTINCT_BITMAP, LSTG_FORMAT_NAME, array['FP-.*GTC', 'Others'], 'REGEXP') as b, "
                + "intersect_count_v2(TEST_COUNT_DISTINCT_BITMAP, LSTG_FORMAT_NAME, array['FP-GTC|FP-non GTC', 'Others'], 'RAWSTRING') as c "
                + "from test_kylin_fact where 1=2";
        QueryParams queryParams = new QueryParams();
        queryParams.setProject(getProject());
        queryParams.setSql(query);
        queryParams.setKylinConfig(getTestConfig());
        queryParams.setSelect(true);
        logger.info("comming....2222 queryRoutingEngine:" + queryRoutingEngine);
        QueryResult result = queryRoutingEngine.queryWithSqlMassage(queryParams);
        List<String> rows = result.getRows().get(0);
        Assert.assertEquals("null", rows.get(0));
        Assert.assertEquals("null", rows.get(1));
    }

}

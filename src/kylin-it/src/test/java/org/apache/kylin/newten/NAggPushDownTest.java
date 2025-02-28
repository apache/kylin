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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NAggPushDownTest extends NLocalWithSparkSessionTest {
    private static final Logger logger = LoggerFactory.getLogger(NAggPushDownTest.class);
    private static final String SQL_FOLDER = "sql_select_subquery";
    private static final String SQL_FOLDER_AGG_NOT_PUSHDOWN = "sql_agg_not_pushdown";
    private static final String JOIN_TYPE = "inner"; // only support inner join

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        overwriteSystemProp("kylin.query.match-partial-inner-join-model", "true");
        overwriteSystemProp("kylin.query.calcite.aggregate-pushdown-enabled", "true");
        setOverlay("src/test/resources/ut_meta/agg_push_down");
        super.setUp();

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContext(getTestConfig());
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/agg_push_down" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        super.tearDown();
    }

    @Override
    public String getProject() {
        return "subquery";
    }

    @Test
    public void testBasic() throws Exception {
        fullBuild("a749e414-c40e-45b7-92e4-bbfe63af705d");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ExecAndComp.CompareLevel compareLevel = ExecAndComp.CompareLevel.SAME;
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());
        String identity = "sqlFolder:" + SQL_FOLDER + ", joinType:" + JOIN_TYPE + ", compareLevel:" + compareLevel;
        try {
            List<Pair<String, String>> queries = ExecAndComp
                    .fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + SQL_FOLDER);
            ExecAndComp.execAndCompare(queries, getProject(), compareLevel, JOIN_TYPE);
            Assert.fail();
        } catch (Throwable th) {
            logger.error("Query fail on: {}", identity);
        }
        logger.info("Query succeed on: {}", identity);

        identity = "sqlFolder:" + SQL_FOLDER_AGG_NOT_PUSHDOWN + ", joinType:" + JOIN_TYPE + ", compareLevel:"
                + compareLevel;
        try {
            List<Pair<String, String>> queries = ExecAndComp
                    .fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + SQL_FOLDER_AGG_NOT_PUSHDOWN);
            ExecAndComp.execAndCompare(queries, getProject(), compareLevel, JOIN_TYPE);
        } catch (Throwable th) {
            logger.error("Query fail on: {}", identity);
            String message = th.getCause().getCause().getCause().getMessage();
            Assert.assertTrue(message.contains("No realization found for OlapContext"));
        }
    }

    @Test
    public void testAggPushDown() throws Exception {
        fullBuild("ce2057da-54c8-4e05-b0bf-d225a6bbb62c");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ExecAndComp.CompareLevel compareLevel = ExecAndComp.CompareLevel.SAME;
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());
        String identity = "sqlFolder:" + "sql_agg_pushdown" + ", joinType:" + JOIN_TYPE + ", compareLevel:"
                + compareLevel;
        try {
            List<Pair<String, String>> queries = ExecAndComp
                    .fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_agg_pushdown");
            ExecAndComp.execAndCompare(queries, getProject(), compareLevel, JOIN_TYPE);
            Assert.fail();
        } catch (Throwable th) {
            logger.error("Query fail on: {}", identity);
        }
        logger.info("Query succeed on: {}", identity);
    }
}

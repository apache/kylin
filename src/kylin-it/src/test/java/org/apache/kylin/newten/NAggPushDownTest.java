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
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.tools.javac.util.Assert;

public class NAggPushDownTest extends NLocalWithSparkSessionTest {
    private static final Logger logger = LoggerFactory.getLogger(NAggPushDownTest.class);
    private String sqlFolder = "sql_select_subquery";
    private String joinType = "inner"; // only support inner join

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("kylin.query.match-partial-inner-join-model", "true");
        overwriteSystemProp("kylin.query.calcite.aggregate-pushdown-enabled", "true");
        this.createTestMetadata("src/test/resources/ut_meta/agg_push_down");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
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
        String identity = "sqlFolder:" + sqlFolder + ", joinType:" + joinType + ", compareLevel:" + compareLevel;
        try {
            List<Pair<String, String>> queries = ExecAndComp
                    .fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + sqlFolder);
            ExecAndComp.execAndCompare(queries, getProject(), compareLevel, joinType);
        } catch (Throwable th) {
            logger.error("Query fail on: {}", identity);
            Assert.error();
        }
        logger.info("Query succeed on: {}", identity);
    }

    @Test
    public void testAggPushDown() throws Exception {
        fullBuild("ce2057da-54c8-4e05-b0bf-d225a6bbb62c");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ExecAndComp.CompareLevel compareLevel = ExecAndComp.CompareLevel.SAME;
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());
        String identity = "sqlFolder:" + "sql_agg_pushdown" + ", joinType:" + joinType + ", compareLevel:" + compareLevel;
        try {
            List<Pair<String, String>> queries = ExecAndComp
                    .fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_agg_pushdown");
            ExecAndComp.execAndCompare(queries, getProject(), compareLevel, joinType);
        } catch (Throwable th) {
            logger.error("Query fail on: {}", identity);
            Assert.error();
        }
        logger.info("Query succeed on: {}", identity);
    }
}

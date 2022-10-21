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

import static org.awaitility.Awaitility.await;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.SlowQueryDetector;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.pushdown.SparkSqlClient;
import org.apache.kylin.query.runtime.plan.ResultPlan;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.AnswersWithDelay;
import org.mockito.internal.stubbing.answers.Returns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class SlowQueryDetectorTest extends NLocalWithSparkSessionTest {
    private SlowQueryDetector slowQueryDetector = null;

    private static final Logger logger = LoggerFactory.getLogger(SlowQueryDetectorTest.class);
    private static final int TIMEOUT_MS = 5 * 1000;

    @Before
    public void setup() {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        createTestMetadata();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        slowQueryDetector = new SlowQueryDetector(100, TIMEOUT_MS);
        slowQueryDetector.start();
    }

    @Override
    public String getProject() {
        return "match";
    }

    @After
    public void after() {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        slowQueryDetector.interrupt();
    }

    @Test
    public void testSetInterrupt() {
        slowQueryDetector.queryStart("");
        try {
            Thread.sleep(6 * 1000);
            Assert.fail();
        } catch (InterruptedException e) {
            Assert.assertEquals("sleep interrupted", e.getMessage());
        }

        slowQueryDetector.queryEnd();
    }

    @Test
    public void testSparderTimeoutCancelJob() throws Exception {
        val df = SparderEnv.getSparkSession().emptyDataFrame();
        val mockDf = Mockito.spy(df);
        Mockito.doAnswer(new AnswersWithDelay(TIMEOUT_MS * 3, new Returns(null))).when(mockDf).toIterator();
        slowQueryDetector.queryStart("");
        try {
            SparderEnv.cleanCompute();
            long t = System.currentTimeMillis();
            ResultPlan.getResult(mockDf, null);
            ExecAndComp.queryModel(getProject(), "select sum(price) from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME");
            String error = "TestSparderTimeoutCancelJob fail, query cost:" + (System.currentTimeMillis() - t)
                    + " ms, need compute:" + SparderEnv.needCompute();
            logger.error(error);
            Assert.fail(error);
        } catch (Exception e) {
            Assert.assertTrue(QueryContext.current().getQueryTagInfo().isTimeout());
            Assert.assertTrue(e instanceof KylinTimeoutException);
            Assert.assertEquals(
                    "The query exceeds the set time limit of 300s. Current step: Collecting dataset for sparder.",
                    e.getMessage());
            // reset query thread's interrupt state.
            Thread.interrupted();
        }
        slowQueryDetector.queryEnd();
    }

    @Test
    public void testPushdownTimeoutCancelJob() {
        val df = SparderEnv.getSparkSession().emptyDataFrame();
        val mockDf = Mockito.spy(df);
        Mockito.doAnswer(new AnswersWithDelay(TIMEOUT_MS * 3, new Returns(null))).when(mockDf).toIterator();
        slowQueryDetector.queryStart("");
        try {
            String sql = "select sum(price) from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME";
            SparkSqlClient.dfToList(ss, "", mockDf);
            SparkSqlClient.executeSql(ss, sql, RandomUtil.randomUUID(), getProject());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(QueryContext.current().getQueryTagInfo().isTimeout());
            Assert.assertTrue(e instanceof KylinTimeoutException);
            Assert.assertEquals(
                    "The query exceeds the set time limit of 300s. Current step: Collecting dataset of push-down.",
                    e.getMessage());
            // reset query thread's interrupt state.
            Thread.interrupted();
        }
        slowQueryDetector.queryEnd();
    }

    @Ignore("not timeout, need another sql")
    @Test
    public void testSQLMassageTimeoutCancelJob() throws Exception {
        slowQueryDetector.queryStart("");
        try {
            SparderEnv.cleanCompute();
            long t = System.currentTimeMillis();
            String sql = FileUtils
                    .readFileToString(new File("src/test/resources/query/sql_timeout/query03.sql"), "UTF-8").trim();
            QueryParams queryParams = new QueryParams(NProjectManager.getProjectConfig(getProject()), sql, getProject(),
                    0, 0, "DEFAULT", true);
            QueryUtil.massageSql(queryParams);
            String error = "TestSQLMassageTimeoutCancelJob fail, query cost:" + (System.currentTimeMillis() - t)
                    + " ms, need compute:" + SparderEnv.needCompute();
            logger.error(error);
            Assert.fail(error);
        } catch (Exception e) {
            Assert.assertTrue(QueryContext.current().getQueryTagInfo().isTimeout());
            Assert.assertTrue(e instanceof KylinTimeoutException);
            Assert.assertTrue(ExceptionUtils.getStackTrace(e).contains("QueryUtil"));
            // reset query thread's interrupt state.
            Thread.interrupted();
        }
        slowQueryDetector.queryEnd();
    }

    @Ignore("TODO: remove or adapt")
    public void testRealizationChooserTimeout() {
        slowQueryDetector.queryStart("");
        try {
            long t = System.currentTimeMillis();
            await().pollDelay(TIMEOUT_MS - 10, TimeUnit.MILLISECONDS).until(() -> true);
            val queryExec = new QueryExec("default", getTestConfig());
            queryExec.executeQuery("select cal_dt,sum(price) from test_kylin_fact group by "
                    + "cal_dt union all select cal_dt,sum(price) from test_kylin_fact group by cal_dt");
            Assert.fail("testRealizationChooserTimeout fail, query cost:" + (System.currentTimeMillis() - t) + " ms");
        } catch (Exception e) {
            Assert.assertTrue(QueryContext.current().getQueryTagInfo().isTimeout());
            Assert.assertTrue(e.getCause() instanceof KylinTimeoutException);
            Assert.assertEquals("KE-000000002", ((KylinTimeoutException) e.getCause()).getErrorCode().getCodeString());
            Assert.assertEquals("The query exceeds the set time limit of 300s. Current step: Realization chooser. ",
                    e.getCause().getMessage());
            Thread.interrupted();
        }
        slowQueryDetector.queryEnd();
    }
}

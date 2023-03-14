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

package org.apache.kylin.query.engine;

import static org.apache.kylin.query.engine.QueryRoutingEngine.SPARK_JOB_FAILED;
import static org.apache.kylin.query.engine.QueryRoutingEngine.SPARK_MEM_LIMIT_EXCEEDED;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.CommonErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.exception.NewQueryRefuseException;
import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.exception.TargetSegmentNotFoundException;
import org.apache.kylin.common.persistence.InMemResourceStore;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.realization.NoStreamingRealizationFoundException;
import org.apache.kylin.query.QueryExtension;
import org.apache.kylin.query.engine.data.QueryResult;
import org.apache.kylin.query.exception.BusyQueryException;
import org.apache.kylin.query.exception.NotSupportedSQLException;
import org.apache.kylin.query.exception.UserStopQueryException;
import org.apache.kylin.query.util.PushDownQueryRequestLimits;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.SlowQueryDetector;
import org.apache.kylin.source.adhocquery.PushdownResult;
import org.apache.spark.SparkException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockSettings;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class QueryRoutingEngineTest extends NLocalFileMetadataTestCase {

    private int pushdownCount;
    @Mock
    private QueryRoutingEngine queryRoutingEngine = Mockito.spy(QueryRoutingEngine.class);

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        pushdownCount = 0;
        // Use default Factory for Open Core
        QueryExtension.setFactory(new QueryExtension.Factory());
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
        // Unset Factory for Open Core
        QueryExtension.setFactory(null);
    }

    @Test
    public void testQueryPushDown() throws Throwable {
        Assert.assertEquals(0, pushdownCount);
        final String sql = "select * from success_table_2";
        final String project = "default";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();

        QueryParams queryParams = new QueryParams();
        queryParams.setProject(project);
        queryParams.setSql(sql);
        queryParams.setKylinConfig(kylinconfig);
        queryParams.setSelect(true);

        Mockito.doAnswer(invocation -> {
            pushdownCount++;
            Assert.assertTrue(ResourceStore.getKylinMetaStore(kylinconfig) instanceof InMemResourceStore);
            return PushdownResult.emptyResult();
        }).when(queryRoutingEngine).tryPushDownSelectQuery(Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        queryRoutingEngine.queryWithSqlMassage(queryParams);
        Assert.assertTrue(QueryContext.current().getQueryTagInfo().isPushdown());
        Assert.assertEquals(1, pushdownCount);

        //to cover force push down
        queryParams.setForcedToPushDown(true);

        Mockito.doAnswer(invocation -> {
            pushdownCount++;
            Assert.assertTrue(ResourceStore.getKylinMetaStore(kylinconfig) instanceof InMemResourceStore);
            return PushdownResult.emptyResult();
        }).when(queryRoutingEngine).tryPushDownSelectQuery(Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        queryRoutingEngine.queryWithSqlMassage(queryParams);
        Assert.assertTrue(QueryContext.current().getQueryTagInfo().isPushdown());
        Assert.assertEquals(2, pushdownCount);

        // Throw Exception When push down
        Mockito.doThrow(new KylinException(QueryErrorCode.SCD2_DUPLICATE_JOIN_COL, "")).when(queryRoutingEngine)
                .tryPushDownSelectQuery(Mockito.any(), Mockito.any(), Mockito.anyBoolean());
        try {
            queryRoutingEngine.queryWithSqlMassage(queryParams);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
        }

        Mockito.doThrow(new Exception("")).when(queryRoutingEngine).tryPushDownSelectQuery(Mockito.any(), Mockito.any(),
                Mockito.anyBoolean());
        try {
            queryRoutingEngine.queryWithSqlMassage(queryParams);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof RuntimeException);
        }

    }

    @Test
    public void testThrowExceptionWhenSparkOOM() throws Exception {
        final String sql = "select * from success_table_2";
        final String project = "default";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();

        QueryParams queryParams = new QueryParams();
        queryParams.setProject(project);
        queryParams.setSql(sql);
        queryParams.setKylinConfig(kylinconfig);
        queryParams.setSelect(true);

        Mockito.doThrow(new SparkException(
                "Job aborted due to stage failure: Task 40 in stage 888.0 failed 1 times, most recent failure: "
                        + "Lost task 40.0 in stage 888.0 (TID 79569, hrbd-73, executor 5): ExecutorLostFailure (executor 5 exited "
                        + "caused by one of the running tasks) Reason: Container killed by YARN for exceeding memory limits.  6.5 GB "
                        + "of 6.5 GB physical memory used. Consider boosting spark.yarn.executor.memoryOverhead or disabling "
                        + "yarn.nodemanager.vmem-check-enabled because of YARN-4714."))
                .when(queryRoutingEngine).execute(Mockito.anyString(), Mockito.any());

        try {
            queryRoutingEngine.queryWithSqlMassage(queryParams);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SparkException || e.getCause() instanceof SparkException);
            Assert.assertTrue(e.getMessage().contains(SPARK_MEM_LIMIT_EXCEEDED)
                    || e.getCause().getMessage().contains(SPARK_MEM_LIMIT_EXCEEDED));
        }
    }

    @Test
    public void testThrowExceptionWhenSparkJobFailed() throws Exception {
        final String sql = "select * from success_table_2";
        final String project = "default";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();

        QueryParams queryParams = new QueryParams();
        queryParams.setProject(project);
        queryParams.setSql(sql);
        queryParams.setKylinConfig(kylinconfig);
        queryParams.setSelect(true);
        //to cover PrepareStatement
        queryParams.setPrepare(true);
        queryParams.setPrepareStatementWithParams(true);
        PrepareSqlStateParam[] params = new PrepareSqlStateParam[11];
        params[0] = new PrepareSqlStateParam(String.class.getName(), "1");
        params[1] = new PrepareSqlStateParam(Integer.class.getName(), "1");
        params[2] = new PrepareSqlStateParam(Short.class.getName(), "1");
        params[3] = new PrepareSqlStateParam(Long.class.getName(), "1");
        params[4] = new PrepareSqlStateParam(Float.class.getName(), "1.1");
        params[5] = new PrepareSqlStateParam(Double.class.getName(), "1.1");
        params[6] = new PrepareSqlStateParam(Boolean.class.getName(), "1");
        params[7] = new PrepareSqlStateParam(Byte.class.getName(), "1");
        params[8] = new PrepareSqlStateParam(Date.class.getName(), "2022-02-22");
        params[9] = new PrepareSqlStateParam(Time.class.getName(), "22:22:22");
        params[10] = new PrepareSqlStateParam(Timestamp.class.getName(), "2022-02-22 22:22:22.22");
        queryParams.setParams(params);

        Mockito.doThrow(
                new TransactionException("", new Throwable(new SparkException("Job aborted due to stage failure: "))))
                .when(queryRoutingEngine).execute(Mockito.anyString(), Mockito.any());

        try {
            queryRoutingEngine.queryWithSqlMassage(queryParams);
        } catch (Exception e) {
            Assert.assertTrue(
                    e.getCause() instanceof SparkException || e.getCause().getCause() instanceof SparkException);
            Assert.assertTrue(e.getCause().getMessage().contains(SPARK_JOB_FAILED)
                    || e.getCause().getCause().getMessage().contains(SPARK_JOB_FAILED));
            Assert.assertFalse(QueryContext.current().getQueryTagInfo().isPushdown());
        }
    }

    @Test
    public void testThrowExceptionWhenNoStreamingRealizationFound() throws Exception {
        final String sql = "select * from success_table_2";
        final String project = "default";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();

        QueryParams queryParams = new QueryParams();
        queryParams.setProject(project);
        queryParams.setSql(sql);
        queryParams.setKylinConfig(kylinconfig);
        queryParams.setSelect(true);

        Mockito.doThrow(new TransactionException("", new Throwable(new NoStreamingRealizationFoundException(""))))
                .when(queryRoutingEngine).execute(Mockito.anyString(), Mockito.any());

        try {
            queryRoutingEngine.queryWithSqlMassage(queryParams);
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof NoStreamingRealizationFoundException
                    || e.getCause().getCause() instanceof NoStreamingRealizationFoundException);
            Assert.assertFalse(QueryContext.current().getQueryTagInfo().isPushdown());
        }
    }

    @Test
    public void testCheckIfRetryQuery() throws Exception {
        //QueryRoutingEngine engine = new QueryRoutingEngine();
        KylinException unrelatedException = new KylinException(QueryErrorCode.BUSY_QUERY,
                "This is an unrelated exception for retry");
        boolean checkResult;
        checkResult = queryRoutingEngine.checkIfRetryQuery(unrelatedException);
        Assert.assertFalse(checkResult);
        TargetSegmentNotFoundException segNotFoundEx = new TargetSegmentNotFoundException("1;2;3");
        checkResult = queryRoutingEngine.checkIfRetryQuery(segNotFoundEx);
        Assert.assertTrue(checkResult);
        // to ensure that query will be retried only once
        checkResult = queryRoutingEngine.checkIfRetryQuery(segNotFoundEx);
        Assert.assertFalse(checkResult);
        QueryContext.current().getMetrics().setRetryTimes(0);

        final String sql = "select * from success_table_2";
        final String project = "default";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();

        QueryParams queryParams = new QueryParams();
        queryParams.setProject(project);
        queryParams.setSql(sql);
        queryParams.setKylinConfig(kylinconfig);
        queryParams.setSelect(true);

        Mockito.doThrow(new SQLException("ex", segNotFoundEx)).when(queryRoutingEngine).execute(Mockito.anyString(),
                Mockito.any());
        try {
            queryRoutingEngine.queryWithSqlMassage(queryParams);
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof TargetSegmentNotFoundException);
            Assert.assertEquals(1, QueryContext.current().getMetrics().getRetryTimes());
        }
        QueryContext.current().getMetrics().setRetryTimes(0);
    }

    @Test
    public void testNewQueryRefuseException() throws Exception {
        final String sql = "select * from success_table_2";
        final String project = "default";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();
        QueryParams queryParams = new QueryParams();
        queryParams.setProject(project);
        queryParams.setSql(sql);
        queryParams.setKylinConfig(kylinconfig);
        queryParams.setSelect(true);

        Mockito.doThrow(new SQLException("",
                new NewQueryRefuseException("Refuse new big query, sum of source_scan_rows is 10, "
                        + "refuse query threshold is 10. Current step: Collecting dataset for sparder. ")))
                .when(queryRoutingEngine).execute(Mockito.anyString(), Mockito.any());

        try {
            queryRoutingEngine.queryWithSqlMassage(queryParams);
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof NewQueryRefuseException);
            Assert.assertFalse(QueryContext.current().getQueryTagInfo().isPushdown());
        }

        kylinconfig.setProperty("kylin.query.share-state-switch-implement", "jdbc");
        kylinconfig.setProperty("kylin.query.big-query-source-scan-rows-threshold", "10");
        kylinconfig.setProperty("kylin.query.big-query-pushdown", "true");
        queryParams.setKylinConfig(kylinconfig);

        Mockito.doThrow(new SQLException("",
                new NewQueryRefuseException("Refuse new big query, sum of source_scan_rows is 10, "
                        + "refuse query threshold is 10. Current step: Collecting dataset for sparder. ")))
                .when(queryRoutingEngine).execute(Mockito.anyString(), Mockito.any());
        try {
            queryRoutingEngine.queryWithSqlMassage(queryParams);
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof NewQueryRefuseException);
            Assert.assertTrue(QueryContext.current().getQueryTagInfo().isPushdown());
        }
        Mockito.doAnswer(invocation -> {
            pushdownCount++;
            Assert.assertTrue(ResourceStore.getKylinMetaStore(kylinconfig) instanceof InMemResourceStore);
            return PushdownResult.emptyResult();
        }).when(queryRoutingEngine).tryPushDownSelectQuery(Mockito.any(), Mockito.any(), Mockito.anyBoolean());
        QueryResult queryResult = queryRoutingEngine.queryWithSqlMassage(queryParams);
        Assert.assertEquals(0, queryResult.getSize());
    }

    @Test
    public void testQueryPushDownWithSumLC() {
        final String sql = "select sUm_Lc \r\n (  \r\n \"success_table_2\".\"column\", \r\n dateColumn) from success_table_2";
        final String project = "default";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();
        QueryParams queryParams = new QueryParams();
        queryParams.setProject(project);
        queryParams.setSql(sql);
        queryParams.setKylinConfig(kylinconfig);
        queryParams.setSelect(true);
        queryParams.setForcedToPushDown(true);

        Assert.assertThrows(NotSupportedSQLException.class, () -> queryRoutingEngine.queryWithSqlMassage(queryParams));
    }

    @Test
    public void testShouldPushDown() {
        final String sql = "select sUm_Lc \r\n (  \r\n \"success_table_2\".\"column\", \r\n dateColumn) from success_table_2";
        final String project = "default";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();
        QueryParams queryParams = new QueryParams();
        queryParams.setProject(project);
        queryParams.setSql(sql);
        queryParams.setKylinConfig(kylinconfig);
        queryParams.setSelect(true);
        queryParams.setForcedToPushDown(true);

        boolean shouldPushDown = queryRoutingEngine.shouldPushdown(new RuntimeException(), queryParams);
        Assert.assertEquals(false, shouldPushDown);
    }

    @Test
    public void testQueryPushDownFail() {
        final String sql = "SELECT 1";
        final String project = "tpch";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();
        kylinconfig.setProperty("kylin.query.timeout-seconds", "5");
        Semaphore semaphore = new Semaphore(0, true);
        try (MockedStatic<PushDownQueryRequestLimits> pushRequest = Mockito
                .mockStatic(PushDownQueryRequestLimits.class)) {
            pushRequest.when((MockedStatic.Verification) PushDownQueryRequestLimits.getSingletonInstance())
                    .thenReturn(semaphore);
            QueryParams queryParams = new QueryParams();
            queryParams.setForcedToPushDown(true);
            queryParams.setProject(project);
            queryParams.setSql(sql);
            queryParams.setKylinConfig(kylinconfig);
            queryParams.setSelect(true);
            QueryRoutingEngine queryRoutingEngine = Mockito.spy(QueryRoutingEngine.class);
            try {
                queryRoutingEngine.tryPushDownSelectQuery(queryParams, null, true);
                Assert.fail("Query rejected. Caused by PushDown query server is too busy");
            } catch (Exception e) {
                Assert.assertTrue(e instanceof BusyQueryException);
                Assert.assertEquals(QueryErrorCode.BUSY_QUERY.toErrorCode(), ((BusyQueryException) e).getErrorCode());
            }
        }
    }

    @Test
    public void testQueryPushDownSuccess() {
        final String sql = "SELECT 1";
        final String project = "tpch";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();
        QueryParams queryParams = new QueryParams();
        queryParams.setForcedToPushDown(true);
        queryParams.setProject(project);
        queryParams.setSql(sql);
        queryParams.setKylinConfig(kylinconfig);
        queryParams.setSelect(true);
        try {
            queryRoutingEngine.tryPushDownSelectQuery(queryParams, null, true);
            Assert.fail("Can't complete the operation. Please check the Spark environment and try again.");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(ServerErrorCode.SPARK_FAILURE.toErrorCode(), ((KylinException) e).getErrorCode());
        }
    }

    @Test
    public void testQueryAsync() {
        final String sql = "SELECT 1";
        final String project = "tpch";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();
        QueryParams queryParams = new QueryParams();
        queryParams.setForcedToPushDown(true);
        queryParams.setProject(project);
        queryParams.setSql(sql);
        queryParams.setKylinConfig(kylinconfig);
        queryParams.setSelect(true);
        try {
            QueryContext.current().getQueryTagInfo().setAsyncQuery(true);
            queryRoutingEngine.tryPushDownSelectQuery(queryParams, null, true);
            Assert.fail("Can't complete the operation. Please check the Spark environment and try again.");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(ServerErrorCode.SPARK_FAILURE.toErrorCode(), ((KylinException) e).getErrorCode());
        }
    }

    @Test
    public void testQueryInterruptedTimeOut() {
        final String sql = "SELECT 1";
        final String project = "tpch";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();
        QueryParams queryParams = new QueryParams();
        queryParams.setForcedToPushDown(true);
        queryParams.setProject(project);
        queryParams.setSql(sql);
        queryParams.setKylinConfig(kylinconfig);
        queryParams.setSelect(true);
        MockSettings mockSettings = Mockito.withSettings().defaultAnswer(Mockito.RETURNS_DEFAULTS);
        Semaphore semaphore = Mockito.mock(Semaphore.class, mockSettings);
        SlowQueryDetector slowQueryDetector = new SlowQueryDetector();
        try (MockedStatic<PushDownQueryRequestLimits> pushRequest = Mockito
                .mockStatic(PushDownQueryRequestLimits.class)) {
            pushRequest.when(PushDownQueryRequestLimits::getSingletonInstance).thenReturn(semaphore);
            try {
                UUID uuid = UUID.randomUUID();
                slowQueryDetector.queryStart(uuid.toString());
                Mockito.doThrow(new InterruptedException()).when(semaphore).tryAcquire(Mockito.anyLong(),
                        Mockito.any(TimeUnit.class));
                QueryRoutingEngine queryRoutingEngine = Mockito.spy(QueryRoutingEngine.class);
                queryRoutingEngine.tryPushDownSelectQuery(queryParams, null, true);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof KylinTimeoutException);
                Assert.assertEquals(CommonErrorCode.TIMEOUT.toErrorCode(), ((KylinException) e).getErrorCode());
            }
        } finally {
            slowQueryDetector.queryEnd();
        }
    }

    @Test
    public void testQueryInterruptedUserStop() {
        final String sql = "SELECT 1";
        final String project = "tpch";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();
        QueryParams queryParams = new QueryParams();
        queryParams.setForcedToPushDown(true);
        queryParams.setProject(project);
        queryParams.setSql(sql);
        queryParams.setKylinConfig(kylinconfig);
        queryParams.setSelect(true);
        MockSettings mockSettings = Mockito.withSettings().defaultAnswer(Mockito.RETURNS_DEFAULTS);
        Semaphore semaphore = Mockito.mock(Semaphore.class, mockSettings);
        SlowQueryDetector slowQueryDetector = new SlowQueryDetector();
        try (MockedStatic<PushDownQueryRequestLimits> pushRequest = Mockito
                .mockStatic(PushDownQueryRequestLimits.class)) {
            pushRequest.when(PushDownQueryRequestLimits::getSingletonInstance).thenReturn(semaphore);
            try {
                UUID uuid = UUID.randomUUID();
                slowQueryDetector.queryStart(uuid.toString());
                SlowQueryDetector.getRunningQueries().get(Thread.currentThread()).setStopByUser(true);
                Mockito.doThrow(new InterruptedException()).when(semaphore).tryAcquire(Mockito.anyLong(),
                        Mockito.any(TimeUnit.class));
                QueryRoutingEngine queryRoutingEngine = Mockito.spy(QueryRoutingEngine.class);
                queryRoutingEngine.tryPushDownSelectQuery(queryParams, null, true);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof UserStopQueryException);
                Assert.assertEquals(QueryErrorCode.USER_STOP_QUERY.toErrorCode(), ((KylinException) e).getErrorCode());
            }
        } finally {
            slowQueryDetector.queryEnd();
        }
    }
}

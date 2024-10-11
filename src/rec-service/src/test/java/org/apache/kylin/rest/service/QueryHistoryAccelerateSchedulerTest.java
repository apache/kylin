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

package org.apache.kylin.rest.service;

import static org.apache.kylin.metadata.favorite.QueryHistoryIdOffset.OffsetType.ACCELERATE;

import java.util.List;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.junit.TimeZoneTestRunner;
import org.apache.kylin.metadata.favorite.AccelerateRuleUtil;
import org.apache.kylin.metadata.favorite.AsyncAccelerationTask;
import org.apache.kylin.metadata.favorite.AsyncTaskManager;
import org.apache.kylin.metadata.favorite.QueryHistoryIdOffsetManager;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.rest.util.QueryHistoryOffsetUtil;
import org.apache.kylin.rest.util.SpringContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.acls.domain.PermissionFactory;
import org.springframework.security.acls.model.PermissionGrantingStrategy;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(TimeZoneTestRunner.class)
@PrepareForTest({ SpringContext.class, UserGroupInformation.class })
@PowerMockIgnore({ "com.sun.security.*", "org.w3c.*", "javax.xml.*", "org.xml.*", "org.w3c.dom.*", "org.apache.cxf.*",
        "javax.management.*", "javax.script.*", "org.apache.hadoop.*", "javax.security.*", "java.security.*",
        "javax.crypto.*", "javax.net.ssl.*", "org.apache.kylin.common.asyncprofiler.AsyncProfiler" })
public class QueryHistoryAccelerateSchedulerTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";
    private static final String DATAFLOW = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
    private static final String LAYOUT1 = "20000000001";
    private static final String LAYOUT2 = "1000001";
    private static final Long QUERY_TIME = 1586760398338L;

    private QueryHistoryAccelerateScheduler qhAccelerateScheduler;
    private JdbcTemplate jdbcTemplate;

    @Mock
    private final IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(SpringContext.class);
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        PermissionFactory permissionFactory = PowerMockito.mock(PermissionFactory.class);
        PermissionGrantingStrategy permissionGrantingStrategy = PowerMockito.mock(PermissionGrantingStrategy.class);
        PowerMockito.when(UserGroupInformation.getCurrentUser())
                .thenAnswer((Answer) invocation -> userGroupInformation);
        createTestMetadata();
        getTestConfig().setProperty("kylin.favorite.query-history-accelerate-interval", "0m");
        ApplicationContext applicationContext = PowerMockito.mock(ApplicationContext.class);
        PowerMockito.when(SpringContext.getApplicationContext()).thenAnswer((Answer) invocation -> applicationContext);
        PowerMockito.when(SpringContext.getBean(PermissionFactory.class))
                .thenAnswer((Answer) invocation -> permissionFactory);
        PowerMockito.when(SpringContext.getBean(PermissionGrantingStrategy.class))
                .thenAnswer((Answer) invocation -> permissionGrantingStrategy);
        qhAccelerateScheduler = Mockito.spy(new QueryHistoryAccelerateScheduler());
        ReflectionTestUtils.setField(qhAccelerateScheduler, "userGroupService", userGroupService);
        jdbcTemplate = JdbcUtil.getJdbcTemplate(getTestConfig());
    }

    @After
    public void tearDown() throws Exception {
        if (jdbcTemplate != null) {
            jdbcTemplate.batchUpdate("SHUTDOWN;");
        }
        cleanupTestMetadata();
    }

    @Test
    public void testQueryAcc() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        qhAccelerateScheduler.queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        qhAccelerateScheduler.accelerateRuleUtil = Mockito.mock(AccelerateRuleUtil.class);
        Mockito.when(qhAccelerateScheduler.queryHistoryDAO.queryQueryHistoriesByIdOffset(Mockito.anyLong(),
                Mockito.anyInt(), Mockito.eq(PROJECT))).thenReturn(queryHistories()).thenReturn(null);

        Mockito.when(qhAccelerateScheduler.accelerateRuleUtil.findMatchedCandidate(Mockito.anyString(),
                Mockito.anyList(), Mockito.anyMap(), Mockito.anyList())).thenReturn(queryHistories());

        // before update id offset
        QueryHistoryIdOffsetManager idOffsetManager = QueryHistoryIdOffsetManager.getInstance(PROJECT);
        Assert.assertEquals(0, idOffsetManager.get(ACCELERATE).getOffset());

        // run update
        QueryHistoryAccelerateScheduler.QueryHistoryAccelerateRunner queryHistoryAccelerateRunner = //
                qhAccelerateScheduler.new QueryHistoryAccelerateRunner(false);
        queryHistoryAccelerateRunner.run();

        // after update id offset
        Assert.assertEquals(8, idOffsetManager.get(ACCELERATE).getOffset());

    }

    @Test
    public void testQueryAccResetOffset() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        qhAccelerateScheduler.queryHistoryDAO = Mockito.spy(RDBMSQueryHistoryDAO.getInstance());
        qhAccelerateScheduler.accelerateRuleUtil = Mockito.mock(AccelerateRuleUtil.class);
        Mockito.when(qhAccelerateScheduler.queryHistoryDAO.queryQueryHistoriesByIdOffset(Mockito.anyLong(),
                Mockito.anyInt(), Mockito.anyString())).thenReturn(Lists.newArrayList());

        // before update id offset
        QueryHistoryIdOffsetManager idOffsetManager = QueryHistoryIdOffsetManager.getInstance(PROJECT);
        idOffsetManager.updateOffset(ACCELERATE, copyForWrite -> copyForWrite.setOffset(999L));
        Assert.assertEquals(999L, idOffsetManager.get(ACCELERATE).getOffset());
        // run update
        QueryHistoryOffsetUtil.resetOffsetId(PROJECT);
        // after auto reset offset
        Assert.assertEquals(0L, idOffsetManager.get(ACCELERATE).getOffset());
    }

    @Test
    public void testQueryAccNotResetOffset() {
        qhAccelerateScheduler.queryHistoryDAO = Mockito.spy(RDBMSQueryHistoryDAO.getInstance());
        qhAccelerateScheduler.accelerateRuleUtil = Mockito.mock(AccelerateRuleUtil.class);
        Mockito.when(qhAccelerateScheduler.queryHistoryDAO.queryQueryHistoriesByIdOffset(Mockito.anyLong(),
                Mockito.anyInt(), Mockito.anyString())).thenReturn(Lists.newArrayList());
        Mockito.when(qhAccelerateScheduler.queryHistoryDAO.getQueryHistoryMaxId(Mockito.anyString())).thenReturn(12L);
        // before update id offset
        QueryHistoryIdOffsetManager idOffsetManager = QueryHistoryIdOffsetManager.getInstance(PROJECT);
        idOffsetManager.updateOffset(ACCELERATE, copyForWrite -> copyForWrite.setOffset(9L));
        Assert.assertEquals(9L, idOffsetManager.get(ACCELERATE).getOffset());

        // run update
        QueryHistoryOffsetUtil.resetOffsetId(PROJECT, qhAccelerateScheduler.queryHistoryDAO);
        // after auto reset offset
        Assert.assertEquals(9L, idOffsetManager.get(ACCELERATE).getOffset());
    }

    @Test
    public void testNotUpdateMetadataForUserTriggered() {
        qhAccelerateScheduler.queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        qhAccelerateScheduler.accelerateRuleUtil = Mockito.mock(AccelerateRuleUtil.class);
        Mockito.when(qhAccelerateScheduler.queryHistoryDAO.queryQueryHistoriesByIdOffset(Mockito.anyLong(),
                Mockito.anyInt(), Mockito.anyString())).thenReturn(queryHistories()).thenReturn(null);
        Mockito.when(qhAccelerateScheduler.accelerateRuleUtil.findMatchedCandidate(Mockito.anyString(),
                Mockito.anyList(), Mockito.anyMap(), Mockito.anyList())).thenReturn(queryHistories());

        // before update id offset
        QueryHistoryIdOffsetManager idOffsetManager = QueryHistoryIdOffsetManager.getInstance(PROJECT);
        Assert.assertEquals(0, idOffsetManager.get(ACCELERATE).getOffset());

        // update sync-acceleration-task to running, then check auto-run will fail
        AsyncTaskManager manager = AsyncTaskManager.getInstance(PROJECT);
        AsyncAccelerationTask asyncTask = (AsyncAccelerationTask) manager.get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
        asyncTask.setAlreadyRunning(true);
        manager.save(asyncTask);
        AsyncAccelerationTask update = (AsyncAccelerationTask) manager.get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
        Assert.assertTrue(update.isAlreadyRunning());

        // run update
        QueryHistoryAccelerateScheduler.QueryHistoryAccelerateRunner queryHistoryAccelerateRunner = //
                qhAccelerateScheduler.new QueryHistoryAccelerateRunner(false);
        queryHistoryAccelerateRunner.run();

        Assert.assertEquals(0, idOffsetManager.get(ACCELERATE).getOffset());
    }

    private List<QueryHistory> queryHistories() {
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSqlPattern("select * from sql1");
        queryHistory1.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory1.setDuration(1000L);
        queryHistory1.setQueryTime(1001);
        queryHistory1.setEngineType("CONSTANTS");
        queryHistory1.setId(1);

        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setSqlPattern("select * from sql2");
        queryHistory2.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory2.setDuration(1000L);
        queryHistory2.setQueryTime(1002);
        queryHistory2.setEngineType("HIVE");
        queryHistory2.setId(2);

        QueryHistory queryHistory3 = new QueryHistory();
        queryHistory3.setSqlPattern("select * from sql3");
        queryHistory3.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory3.setDuration(1000L);
        queryHistory3.setQueryTime(1003);
        queryHistory3.setEngineType("NATIVE");
        queryHistory3.setId(3);

        QueryHistory queryHistory4 = new QueryHistory();
        queryHistory4.setSqlPattern("select * from sql3");
        queryHistory4.setQueryStatus(QueryHistory.QUERY_HISTORY_FAILED);
        queryHistory4.setDuration(1000L);
        queryHistory4.setQueryTime(1004);
        queryHistory4.setEngineType("HIVE");
        queryHistory4.setId(4);

        QueryHistory queryHistory5 = new QueryHistory();
        queryHistory5.setSqlPattern("SELECT \"LSTG_FORMAT_NAME\"\n" + "FROM \"KYLIN_SALES\"");
        queryHistory5.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory5.setDuration(1000L);
        queryHistory5.setQueryTime(QUERY_TIME);
        queryHistory5.setEngineType("NATIVE");
        QueryHistoryInfo queryHistoryInfo5 = new QueryHistoryInfo();
        queryHistoryInfo5.setRealizationMetrics(Lists.newArrayList(
                new QueryMetrics.RealizationMetrics(LAYOUT1, "Table Index", DATAFLOW, Lists.newArrayList())));
        queryHistory5.setQueryHistoryInfo(queryHistoryInfo5);
        queryHistory5.setId(5);

        QueryHistory queryHistory6 = new QueryHistory();
        queryHistory6.setSqlPattern("SELECT \"LSTG_FORMAT_NAME\"\n" + "FROM \"KYLIN_SALES\"");
        queryHistory6.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory6.setDuration(1000L);
        queryHistory6.setQueryTime(QUERY_TIME);
        queryHistory6.setEngineType("NATIVE");
        QueryHistoryInfo queryHistoryInfo6 = new QueryHistoryInfo();
        queryHistoryInfo6.setRealizationMetrics(Lists.newArrayList(
                new QueryMetrics.RealizationMetrics(LAYOUT1, "Table Index", DATAFLOW, Lists.newArrayList())));
        queryHistory6.setQueryHistoryInfo(queryHistoryInfo6);
        queryHistory6.setId(6);

        QueryHistory queryHistory7 = new QueryHistory();
        queryHistory7.setSqlPattern("SELECT count(*) FROM \"KYLIN_SALES\" group by LSTG_FORMAT_NAME");
        queryHistory7.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory7.setDuration(1000L);
        queryHistory7.setQueryTime(QUERY_TIME);
        queryHistory7.setEngineType("NATIVE");
        QueryHistoryInfo queryHistoryInfo7 = new QueryHistoryInfo();
        queryHistoryInfo7.setRealizationMetrics(Lists.newArrayList(
                new QueryMetrics.RealizationMetrics(LAYOUT2, "Table Index", DATAFLOW, Lists.newArrayList())));
        queryHistory7.setQueryHistoryInfo(queryHistoryInfo7);
        queryHistory7.setId(7);

        QueryHistory queryHistory8 = new QueryHistory();
        queryHistory8.setSqlPattern("SELECT count(*) FROM \"KYLIN_SALES\" group by LSTG_FORMAT_NAME");
        queryHistory8.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory8.setDuration(1000L);
        queryHistory8.setQueryTime(QUERY_TIME);
        queryHistory8.setEngineType("NATIVE");
        QueryHistoryInfo queryHistoryInfo8 = new QueryHistoryInfo();
        queryHistoryInfo8.setRealizationMetrics(
                Lists.newArrayList(new QueryMetrics.RealizationMetrics(null, null, DATAFLOW, Lists.newArrayList())));
        queryHistory8.setQueryHistoryInfo(queryHistoryInfo8);
        queryHistory8.setId(8);

        List<QueryHistory> histories = Lists.newArrayList(queryHistory1, queryHistory2, queryHistory3, queryHistory4,
                queryHistory5, queryHistory6, queryHistory7, queryHistory8);

        for (QueryHistory history : histories) {
            history.setId(startOffset + history.getId());
        }
        startOffset += histories.size();

        return histories;
    }

    int startOffset = 0;

}

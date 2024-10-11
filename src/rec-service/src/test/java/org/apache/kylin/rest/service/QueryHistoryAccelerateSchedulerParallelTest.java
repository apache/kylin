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
import static org.apache.kylin.metadata.query.RDBMSQueryHistoryDaoTest.createQueryMetrics;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.junit.TimeZoneTestRunner;
import org.apache.kylin.metadata.favorite.QueryHistoryIdOffsetManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
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

import lombok.var;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(TimeZoneTestRunner.class)
@PrepareForTest({ SpringContext.class, UserGroupInformation.class })
@PowerMockIgnore({ "com.sun.security.*", "org.w3c.*", "javax.xml.*", "org.xml.*", "org.w3c.dom.*", "org.apache.cxf.*",
        "javax.management.*", "javax.script.*", "org.apache.hadoop.*", "javax.security.*", "java.security.*",
        "javax.crypto.*", "javax.net.ssl.*", "org.apache.kylin.common.asyncprofiler.AsyncProfiler" })
public class QueryHistoryAccelerateSchedulerParallelTest extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "default";

    private QueryHistoryAccelerateScheduler qhAccelerateScheduler;
    private QueryHistoryAccelerateScheduler qhAccelerateScheduler2;
    private RDBMSQueryHistoryDAO queryHistoryDAO;
    private final ScheduledExecutorService taskScheduler = Executors.newScheduledThreadPool(2,
            new NamedThreadFactory("QueryHistoryTestWorker"));
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
        ApplicationContext applicationContext = PowerMockito.mock(ApplicationContext.class);
        PowerMockito.when(SpringContext.getApplicationContext()).thenAnswer((Answer) invocation -> applicationContext);
        PowerMockito.when(SpringContext.getBean(PermissionFactory.class))
                .thenAnswer((Answer) invocation -> permissionFactory);
        PowerMockito.when(SpringContext.getBean(PermissionGrantingStrategy.class))
                .thenAnswer((Answer) invocation -> permissionGrantingStrategy);
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        qhAccelerateScheduler = Mockito.spy(new QueryHistoryAccelerateScheduler());
        qhAccelerateScheduler2 = Mockito.spy(new QueryHistoryAccelerateScheduler());
        ReflectionTestUtils.setField(qhAccelerateScheduler, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(qhAccelerateScheduler2, "userGroupService", userGroupService);

        getTestConfig().setProperty("kylin.query.queryhistory.url",
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1;MODE=MYSQL,username=sa,password=");
        getTestConfig().setProperty("kylin.query.query-history-stat-update-max-size", "5");
        getTestConfig().setProperty("kylin.favorite.query-history-accelerate-max-size", "5");
        getTestConfig().setProperty("kylin.query.query-history-stat-batch-size", "2");
        getTestConfig().setProperty("kylin.favorite.query-history-accelerate-batch-size", "2");
        List<QueryMetrics> queryMetricsList = Lists.newArrayList();
        for (int i = 0; i < 100; i++) {
            queryMetricsList.add(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        }
        queryHistoryDAO.insert(queryMetricsList);
        qhAccelerateScheduler.queryHistoryDAO = queryHistoryDAO;
        qhAccelerateScheduler2.queryHistoryDAO = queryHistoryDAO;

        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        projectManager.updateProject(PROJECT, copyForWrite -> {
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kylin.metadata.semi-automatic-mode", "true");
            copyForWrite.setOverrideKylinProps(properties);
        });
        jdbcTemplate = JdbcUtil.getJdbcTemplate(getTestConfig());
    }

    @After
    public void tearDown() throws Exception {
        queryHistoryDAO.dropQueryHistoryTable();
        if (jdbcTemplate != null) {
            jdbcTemplate.batchUpdate("SHUTDOWN;");
        }
        cleanupTestMetadata();
    }

    @Test
    public void testParallelUpdateWithMetadataConflict() {
        QueryHistoryAccelerateScheduler.QueryHistoryAccelerateRunner accelerateRunner = getAccelerateRunner(
                qhAccelerateScheduler, false);
        QueryHistoryAccelerateScheduler.QueryHistoryAccelerateRunner accelerateRunner2 = getAccelerateRunner(
                qhAccelerateScheduler2, false);

        taskScheduler.schedule(accelerateRunner, 0, TimeUnit.SECONDS);
        taskScheduler.schedule(accelerateRunner2, 0, TimeUnit.SECONDS);

        await().atMost(7, TimeUnit.SECONDS).until(() -> ((ThreadPoolExecutor) taskScheduler).getActiveCount() == 0);
        QueryHistoryIdOffsetManager offsetManager = QueryHistoryIdOffsetManager.getInstance(PROJECT);
        Assert.assertEquals(12, offsetManager.get(ACCELERATE).getOffset());
    }

    @Test
    public void testParallelUpdateWithUpdateGapLessThenScheduleInterval() {
        QueryHistoryAccelerateScheduler.QueryHistoryAccelerateRunner accelerateRunner = getAccelerateRunner(
                qhAccelerateScheduler, false);
        QueryHistoryAccelerateScheduler.QueryHistoryAccelerateRunner accelerateRunner2 = getAccelerateRunner(
                qhAccelerateScheduler2, false);

        taskScheduler.schedule(accelerateRunner, 0, TimeUnit.SECONDS);
        taskScheduler.schedule(accelerateRunner2, 1, TimeUnit.SECONDS);

        await().atMost(7, TimeUnit.SECONDS).until(() -> ((ThreadPoolExecutor) taskScheduler).getActiveCount() == 0);
        QueryHistoryIdOffsetManager offsetManager = QueryHistoryIdOffsetManager.getInstance(PROJECT);
        Assert.assertEquals(6, offsetManager.get(ACCELERATE).getOffset());
    }

    @Test
    public void testParallelUpdate() {
        QueryHistoryAccelerateScheduler.QueryHistoryAccelerateRunner accelerateRunner = getAccelerateRunner(
                qhAccelerateScheduler, false);
        QueryHistoryAccelerateScheduler.QueryHistoryAccelerateRunner accelerateRunner2 = getAccelerateRunner(
                qhAccelerateScheduler2, false);

        getTestConfig().setProperty("kylin.favorite.query-history-accelerate-interval", "0m");
        taskScheduler.schedule(accelerateRunner, 0, TimeUnit.SECONDS);
        taskScheduler.schedule(accelerateRunner2, 1, TimeUnit.SECONDS);

        await().atMost(5, TimeUnit.SECONDS).until(() -> ((ThreadPoolExecutor) taskScheduler).getActiveCount() == 0);
        QueryHistoryIdOffsetManager offsetManager = QueryHistoryIdOffsetManager.getInstance(PROJECT);
        Assert.assertEquals(12, offsetManager.get(ACCELERATE).getOffset());
    }

    @Test
    public void testAutoAndManualParallelUpdate() {
        QueryHistoryAccelerateScheduler.QueryHistoryAccelerateRunner accelerateRunner = getAccelerateRunner(
                qhAccelerateScheduler, false);
        QueryHistoryAccelerateScheduler.QueryHistoryAccelerateRunner accelerateRunner2 = getAccelerateRunner(
                qhAccelerateScheduler2, true);

        taskScheduler.schedule(accelerateRunner, 0, TimeUnit.SECONDS);
        taskScheduler.schedule(accelerateRunner2, 1, TimeUnit.SECONDS);

        ThreadPoolExecutor poolExecutor = (ThreadPoolExecutor) taskScheduler;
        await().atMost(5, TimeUnit.SECONDS).until(() -> poolExecutor.getActiveCount() == 0);
        QueryHistoryIdOffsetManager offsetManager = QueryHistoryIdOffsetManager.getInstance(PROJECT);
        Assert.assertEquals(8, offsetManager.get(ACCELERATE).getOffset());
    }

    @Test
    public void testManualAndManualParallelUpdate() {
        QueryHistoryAccelerateScheduler.QueryHistoryAccelerateRunner accelerateRunner = getAccelerateRunner(
                qhAccelerateScheduler, true);
        QueryHistoryAccelerateScheduler.QueryHistoryAccelerateRunner accelerateRunner2 = getAccelerateRunner(
                qhAccelerateScheduler2, true);

        taskScheduler.schedule(accelerateRunner, 0, TimeUnit.SECONDS);
        taskScheduler.schedule(accelerateRunner2, 1, TimeUnit.SECONDS);

        await().atMost(5, TimeUnit.SECONDS).until(() -> ((ThreadPoolExecutor) taskScheduler).getActiveCount() == 0);
        QueryHistoryIdOffsetManager offsetManager = QueryHistoryIdOffsetManager.getInstance(PROJECT);
        Assert.assertEquals(4, offsetManager.get(ACCELERATE).getOffset());
    }

    QueryHistoryAccelerateScheduler.QueryHistoryAccelerateRunner getAccelerateRunner(
            QueryHistoryAccelerateScheduler scheduler, boolean isManual) {
        return scheduler.new QueryHistoryAccelerateRunner(isManual) {
            @Override
            public void accelerateAndUpdateMetadata(Pair<List<QueryHistory>, String> pair) {
                await().pollDelay(1, TimeUnit.SECONDS).until(() -> true);
                super.accelerateAndUpdateMetadata(pair);
            }
        };
    }

}

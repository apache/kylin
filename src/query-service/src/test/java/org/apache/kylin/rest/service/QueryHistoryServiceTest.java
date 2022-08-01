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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.ProcessUtils;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.query.NativeQueryRealization;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryHistoryRequest;
import org.apache.kylin.metadata.query.QueryHistorySql;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.query.QueryStatistics;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDaoTest;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.response.QueryStatisticsResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;

import lombok.val;
import lombok.var;

public class QueryHistoryServiceTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";

    @InjectMocks
    private final QueryHistoryService queryHistoryService = Mockito.spy(new QueryHistoryService());

    @Mock
    private final ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private final TableService tableService = Mockito.spy(new TableService());
    @InjectMocks
    private final JobService jobService = Mockito.spy(new JobService());
    @Mock
    private final AclTCRService aclTCRService = Mockito.spy(AclTCRService.class);
    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);
    @InjectMocks
    private FusionModelService fusionModelService = Mockito.spy(new FusionModelService());

    @Before
    public void setUp() {
        createTestMetadata();
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(tableService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableService, "modelService", modelService);
        ReflectionTestUtils.setField(tableService, "fusionModelService", fusionModelService);
        ReflectionTestUtils.setField(tableService, "aclTCRService", aclTCRService);
        ReflectionTestUtils.setField(tableService, "jobService", jobService);
        ReflectionTestUtils.setField(queryHistoryService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(queryHistoryService, "modelService", modelService);
        ReflectionTestUtils.setField(queryHistoryService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(queryHistoryService, "asyncTaskService", new AsyncTaskService());
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
    }

    @After
    public void tearDown() {
        RDBMSQueryHistoryDAO.getInstance().deleteAllQueryHistory();
        cleanupTestMetadata();
    }

    @Test
    public void testGetFilteredQueryHistories() {
        // when there is no filter conditions
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        // set default values
        request.setStartTimeFrom("0");
        request.setStartTimeTo(String.valueOf(Long.MAX_VALUE));
        request.setLatencyFrom("0");
        request.setLatencyTo(String.valueOf(Integer.MAX_VALUE));

        // mock query histories
        // pushdown query
        QueryHistory pushdownQuery = new QueryHistory();
        pushdownQuery.setSql("select * from test_table_1");
        pushdownQuery.setEngineType("HIVE");

        // failed query
        QueryHistory failedQuery = new QueryHistory();
        failedQuery.setSql("select * from test_table_2");

        // accelerated query
        QueryHistory acceleratedQuery = new QueryHistory();
        acceleratedQuery.setSql("select * from test_table_3");
        QueryMetrics.RealizationMetrics metrics1 = new QueryMetrics.RealizationMetrics("1", "Agg Index",
                "741ca86a-1f13-46da-a59f-95fb68615e3a", Lists.newArrayList(new String[] {}));
        QueryMetrics.RealizationMetrics metrics2 = new QueryMetrics.RealizationMetrics("1", "Agg Index",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", Lists.newArrayList(new String[] {}));
        QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo();
        queryHistoryInfo.setRealizationMetrics(
                Lists.newArrayList(new QueryMetrics.RealizationMetrics[] { metrics1, metrics2 }));
        acceleratedQuery.setQueryHistoryInfo(queryHistoryInfo);

        RDBMSQueryHistoryDAO queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(pushdownQuery, failedQuery, acceleratedQuery)).when(queryHistoryDAO)
                .getQueryHistoriesByConditions(Mockito.any(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(10L).when(queryHistoryDAO).getQueryHistoriesSize(Mockito.any(), Mockito.anyString());
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao();

        Map<String, Object> result = queryHistoryService.getQueryHistories(request, 10, 0);
        List<QueryHistory> queryHistories = (List<QueryHistory>) result.get("query_histories");
        long size = (long) result.get("size");

        Assert.assertEquals(3, queryHistories.size());
        Assert.assertEquals(10, size);

        // assert pushdown query
        Assert.assertEquals(pushdownQuery.getSql(), queryHistories.get(0).getSql());
        Assert.assertEquals(pushdownQuery.getEngineType(), queryHistories.get(0).getEngineType());
        Assert.assertTrue(CollectionUtils.isEmpty(pushdownQuery.getNativeQueryRealizations()));
        // assert failed query
        Assert.assertEquals(failedQuery.getSql(), queryHistories.get(1).getSql());
        Assert.assertTrue(CollectionUtils.isEmpty(queryHistories.get(1).getNativeQueryRealizations()));
        Assert.assertNull(queryHistories.get(1).getEngineType());
        // assert accelerated query
        Assert.assertEquals(acceleratedQuery.getSql(), queryHistories.get(2).getSql());
        var modelAlias = queryHistories.get(2).getNativeQueryRealizations().stream()
                .map(NativeQueryRealization::getModelAlias).collect(Collectors.toSet());
        Assert.assertEquals(2, modelAlias.size());
        Assert.assertTrue(modelAlias.contains("nmodel_basic"));
        Assert.assertTrue(modelAlias.contains("nmodel_basic_inner"));

        val modelIds = queryHistories.get(2).getNativeQueryRealizations().stream()
                .map(NativeQueryRealization::getModelId).collect(Collectors.toSet());
        Assert.assertTrue(modelIds.contains("741ca86a-1f13-46da-a59f-95fb68615e3a"));
        Assert.assertTrue(modelIds.contains("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));

        tableService.unloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", false);
        queryHistories = (List<QueryHistory>) queryHistoryService.getQueryHistories(request, 10, 0)
                .get("query_histories");
        modelAlias = queryHistories.get(2).getNativeQueryRealizations().stream()
                .map(NativeQueryRealization::getModelAlias).collect(Collectors.toSet());
        Assert.assertTrue(modelAlias.contains("nmodel_basic broken"));
        Assert.assertTrue(modelAlias.contains("nmodel_basic_inner broken"));

        val id = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        NDataflowManager.getInstance(getTestConfig(), PROJECT).dropDataflow(id);
        NIndexPlanManager.getInstance(getTestConfig(), PROJECT).dropIndexPlan(id);
        NDataModelManager.getInstance(getTestConfig(), PROJECT).dropModel(id);
        queryHistories = (List<QueryHistory>) queryHistoryService.getQueryHistories(request, 10, 0)
                .get("query_histories");
        modelAlias = queryHistories.get(2).getNativeQueryRealizations().stream()
                .map(NativeQueryRealization::getModelAlias).collect(Collectors.toSet());
        Assert.assertTrue(modelAlias.contains(QueryHistoryService.DELETED_MODEL));

    }

    @Test
    public void testGetQueryStatistics() {
        QueryStatistics queryStatistics = new QueryStatistics();
        queryStatistics.setCount(100);
        queryStatistics.setMeanDuration(500);

        RDBMSQueryHistoryDAO queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        Mockito.doReturn(queryStatistics).when(queryHistoryDAO).getQueryCountAndAvgDuration(0, Long.MAX_VALUE,
                "default");
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao();

        QueryStatisticsResponse result = queryHistoryService.getQueryStatistics(PROJECT, 0, Long.MAX_VALUE);
        Assert.assertEquals(100, result.getCount());
        Assert.assertEquals(500, result.getMean(), 0.1);
    }

    @Test
    public void testGetQueryCount() throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault(Locale.Category.FORMAT));
        long startTime = format.parse("2018-01-01").getTime();
        long endTime = format.parse("2018-01-03").getTime();

        RDBMSQueryHistoryDAO queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        Mockito.doReturn(getTestStatistics()).when(queryHistoryDAO).getQueryCountByModel(startTime, endTime, "default");
        Mockito.doReturn(getTestStatistics()).when(queryHistoryDAO).getQueryCountByTime(Mockito.anyLong(),
                Mockito.anyLong(), Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao();

        // query count by model
        Map<String, Object> result = queryHistoryService.getQueryCount(PROJECT, startTime, endTime, "model");
        Assert.assertEquals(3, result.size());
        Assert.assertEquals(10L, result.get("nmodel_basic"));
        Assert.assertEquals(11L, result.get("all_fixed_length"));
        Assert.assertEquals(12L, result.get("test_encoding"));

        // query count by day
        result = queryHistoryService.getQueryCount(PROJECT, startTime, endTime, "day");
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(10L, result.get("2018-01-01"));
        Assert.assertEquals(11L, result.get("2018-01-02"));
        Assert.assertEquals(12L, result.get("2018-01-03"));

        // query count by week
        result = queryHistoryService.getQueryCount(PROJECT, startTime, endTime, "week");
        Assert.assertEquals(5, result.size());
        Assert.assertEquals(10L, result.get("2018-01-01"));
        Assert.assertEquals(11L, result.get("2018-01-02"));
        Assert.assertEquals(12L, result.get("2018-01-03"));

        // query count by month
        result = queryHistoryService.getQueryCount(PROJECT, startTime, endTime, "month");
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(11L, result.get("2018-01"));
    }

    @Test
    public void testGetAvgDuration() throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault(Locale.Category.FORMAT));
        long startTime = format.parse("2018-01-01").getTime();
        long endTime = format.parse("2018-01-03").getTime();

        RDBMSQueryHistoryDAO queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        Mockito.doReturn(getTestStatistics()).when(queryHistoryDAO).getAvgDurationByModel(startTime, endTime,
                "default");
        Mockito.doReturn(getTestStatistics()).when(queryHistoryDAO).getAvgDurationByTime(Mockito.anyLong(),
                Mockito.anyLong(), Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao();

        // avg duration by model
        Map<String, Object> result = queryHistoryService.getAvgDuration(PROJECT, startTime, endTime, "model");
        Assert.assertEquals(3, result.size());
        Assert.assertEquals(500, (double) result.get("nmodel_basic"), 0.1);
        Assert.assertEquals(600, (double) result.get("all_fixed_length"), 0.1);
        Assert.assertEquals(700, (double) result.get("test_encoding"), 0.1);

        // avg duration by day
        result = queryHistoryService.getAvgDuration(PROJECT, startTime, endTime, "day");
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(500, (double) result.get("2018-01-01"), 0.1);
        Assert.assertEquals(600, (double) result.get("2018-01-02"), 0.1);
        Assert.assertEquals(700, (double) result.get("2018-01-03"), 0.1);

        // avg duration by week
        result = queryHistoryService.getAvgDuration(PROJECT, startTime, endTime, "week");
        Assert.assertEquals(5, result.size());
        Assert.assertEquals(500, (double) result.get("2018-01-01"), 0.1);
        Assert.assertEquals(600, (double) result.get("2018-01-02"), 0.1);
        Assert.assertEquals(700, (double) result.get("2018-01-03"), 0.1);

        // avg duration by month
        result = queryHistoryService.getAvgDuration(PROJECT, 0, endTime, "month");
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(600, (double) result.get("2018-01"), 0.1);
    }

    private List<QueryStatistics> getTestStatistics() throws ParseException {
        int rawOffsetTime = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone()).getRawOffset();
        String date = "2018-01-01";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault(Locale.Category.FORMAT));
        long time = format.parse(date).getTime();

        QueryStatistics queryStatistics1 = new QueryStatistics();
        queryStatistics1.setCount(10);
        queryStatistics1.setMeanDuration(500);
        queryStatistics1.setModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        queryStatistics1.setTime(Instant.ofEpochMilli(time + rawOffsetTime));
        queryStatistics1.setMonth(date);

        date = "2018-01-02";
        time = format.parse(date).getTime();

        QueryStatistics queryStatistics2 = new QueryStatistics();
        queryStatistics2.setCount(11);
        queryStatistics2.setMeanDuration(600);
        queryStatistics2.setModel("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        queryStatistics2.setTime(Instant.ofEpochMilli(time + rawOffsetTime));
        queryStatistics2.setMonth(date);

        date = "2018-01-03";
        time = format.parse(date).getTime();

        QueryStatistics queryStatistics3 = new QueryStatistics();
        queryStatistics3.setCount(12);
        queryStatistics3.setMeanDuration(700);
        queryStatistics3.setModel("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94");
        queryStatistics3.setTime(Instant.ofEpochMilli(time + rawOffsetTime));
        queryStatistics3.setMonth(date);

        date = "2018-01-04";
        time = format.parse(date).getTime();
        QueryStatistics queryStatistics4 = new QueryStatistics();
        queryStatistics4.setCount(11);
        queryStatistics4.setMeanDuration(600);
        queryStatistics4.setModel("not_existing_model");
        queryStatistics4.setTime(Instant.ofEpochMilli(time + rawOffsetTime));
        queryStatistics4.setMonth(date);

        return Lists.newArrayList(queryStatistics1, queryStatistics2, queryStatistics3, queryStatistics4);
    }

    @Test
    public void testGetQueryHistoryTableNames() {
        List<String> projects = Lists.newArrayList(PROJECT, "newten");
        Map<String, String> tableMap = queryHistoryService.getQueryHistoryTableMap(projects);
        Assert.assertEquals(2, tableMap.size());
        Assert.assertEquals("_examples_test_data_" + ProcessUtils.getCurrentId("0") + "_metadata_query_history",
                tableMap.get("newten"));
        Assert.assertEquals("_examples_test_data_" + ProcessUtils.getCurrentId("0") + "_metadata_query_history",
                tableMap.get(PROJECT));

        // get all tables
        tableMap = queryHistoryService.getQueryHistoryTableMap(null);
        Assert.assertEquals(26, tableMap.size());

        // not existing project
        try {
            tableMap = queryHistoryService.getQueryHistoryTableMap(Lists.newArrayList("not_existing_project"));
        } catch (Exception ex) {
            Assert.assertEquals(BadRequestException.class, ex.getClass());
            Assert.assertEquals("Cannot find project 'not_existing_project'.", ex.getMessage());
        }
    }

    @Test
    public void testGetQueryHistoryNullLayoutId() {
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        // set default values
        request.setStartTimeFrom("0");
        request.setStartTimeTo(String.valueOf(Long.MAX_VALUE));
        request.setLatencyFrom("0");
        request.setLatencyTo(String.valueOf(Integer.MAX_VALUE));

        // mock query histories
        QueryHistory layoutNullQuery = new QueryHistory();
        layoutNullQuery.setSql("select * from test_table_1");
        layoutNullQuery.setEngineType("NATIVE");
        QueryMetrics.RealizationMetrics nullQueryMetrics1 = new QueryMetrics.RealizationMetrics(null, null,
                "741ca86a-1f13-46da-a59f-95fb68615e3a", Lists.newArrayList(new String[] {}));
        QueryMetrics.RealizationMetrics nullQueryMetrics2 = new QueryMetrics.RealizationMetrics("1", "Agg Index",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", Lists.newArrayList(new String[] {}));
        QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo();
        queryHistoryInfo.setRealizationMetrics(
                Lists.newArrayList(new QueryMetrics.RealizationMetrics[] { nullQueryMetrics1, nullQueryMetrics2 }));
        layoutNullQuery.setQueryHistoryInfo(queryHistoryInfo);

        // accelerated query
        QueryHistory acceleratedQuery = new QueryHistory();
        acceleratedQuery.setSql("select * from test_table_3");
        QueryMetrics.RealizationMetrics metrics1 = new QueryMetrics.RealizationMetrics("1", "Agg Index",
                "741ca86a-1f13-46da-a59f-95fb68615e3a", Lists.newArrayList(new String[] {}));
        QueryMetrics.RealizationMetrics metrics2 = new QueryMetrics.RealizationMetrics("1", "Agg Index",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", Lists.newArrayList(new String[] {}));
        QueryHistoryInfo queryHistoryInfo2 = new QueryHistoryInfo();
        queryHistoryInfo2.setRealizationMetrics(
                Lists.newArrayList(new QueryMetrics.RealizationMetrics[] { metrics1, metrics2 }));
        acceleratedQuery.setQueryHistoryInfo(queryHistoryInfo2);

        RDBMSQueryHistoryDAO queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(layoutNullQuery, acceleratedQuery)).when(queryHistoryDAO)
                .getQueryHistoriesByConditions(Mockito.any(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(10L).when(queryHistoryDAO).getQueryHistoriesSize(Mockito.any(), Mockito.anyString());
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao();

        Map<String, Object> result = queryHistoryService.getQueryHistories(request, 10, 0);
        List<QueryHistory> queryHistories = (List<QueryHistory>) result.get("query_histories");
        Assert.assertEquals(2, queryHistories.size());
        Assert.assertNull(queryHistories.get(0).getNativeQueryRealizations().get(0).getLayoutId());
        Assert.assertNull(queryHistories.get(0).getNativeQueryRealizations().get(0).getIndexType());
        Assert.assertEquals("nmodel_basic", queryHistories.get(0).getNativeQueryRealizations().get(1).getModelAlias());
        Assert.assertEquals(1L, (long) queryHistories.get(1).getNativeQueryRealizations().get(0).getLayoutId());
        Assert.assertEquals(1L, (long) queryHistories.get(1).getNativeQueryRealizations().get(1).getLayoutId());
    }

    @Test
    public void testGetQueryHistoryWithoutSnapshotInfo() {
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        // set default values
        request.setStartTimeFrom("0");
        request.setStartTimeTo(String.valueOf(Long.MAX_VALUE));
        request.setLatencyFrom("0");
        request.setLatencyTo(String.valueOf(Integer.MAX_VALUE));

        // mock query histories
        QueryHistory noSnapshotQuery = new QueryHistory();
        noSnapshotQuery.setSql("select * from test_table_1");
        noSnapshotQuery.setEngineType("NATIVE");
        QueryMetrics.RealizationMetrics noSnapshotMetrics1 = new QueryMetrics.RealizationMetrics("1", "Agg Index",
                "741ca86a-1f13-46da-a59f-95fb68615e3a", Lists.newArrayList(new String[] {}));
        QueryMetrics.RealizationMetrics noSnapshotMetrics2 = new QueryMetrics.RealizationMetrics("1", "Agg Index",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", Lists.newArrayList(new String[] { "test_snapshot" }));
        QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo();
        queryHistoryInfo.setRealizationMetrics(
                Lists.newArrayList(new QueryMetrics.RealizationMetrics[] { noSnapshotMetrics1, noSnapshotMetrics2 }));
        noSnapshotQuery.setQueryHistoryInfo(queryHistoryInfo);

        QueryHistory containSnapshotQuery = new QueryHistory();
        containSnapshotQuery.setSql("select * from test_table_3");
        QueryMetrics.RealizationMetrics metrics1 = new QueryMetrics.RealizationMetrics("1", "Agg Index",
                "741ca86a-1f13-46da-a59f-95fb68615e3a", Lists.newArrayList(new String[] {}));
        QueryMetrics.RealizationMetrics metrics2 = new QueryMetrics.RealizationMetrics("1", "Agg Index",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", Lists.newArrayList(new String[] { "test_snapshot" }));
        QueryHistoryInfo queryHistoryInfo2 = new QueryHistoryInfo();
        queryHistoryInfo2.setRealizationMetrics(
                Lists.newArrayList(new QueryMetrics.RealizationMetrics[] { metrics1, metrics2 }));
        containSnapshotQuery.setQueryHistoryInfo(queryHistoryInfo2);

        RDBMSQueryHistoryDAO queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(noSnapshotQuery, containSnapshotQuery)).when(queryHistoryDAO)
                .getQueryHistoriesByConditions(Mockito.any(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(10L).when(queryHistoryDAO).getQueryHistoriesSize(Mockito.any(), Mockito.anyString());
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao();

        Map<String, Object> result = queryHistoryService.getQueryHistories(request, 10, 0);
        List<QueryHistory> queryHistories = (List<QueryHistory>) result.get("query_histories");
        Assert.assertEquals(2, queryHistories.size());
        Assert.assertEquals("nmodel_basic", queryHistories.get(0).getNativeQueryRealizations().get(1).getModelAlias());
        Assert.assertEquals(1L, (long) queryHistories.get(1).getNativeQueryRealizations().get(0).getLayoutId());
        Assert.assertEquals(1L, (long) queryHistories.get(1).getNativeQueryRealizations().get(1).getLayoutId());
        Assert.assertEquals(0, queryHistories.get(0).getNativeQueryRealizations().get(0).getSnapshots().size());
        Assert.assertEquals(1, queryHistories.get(0).getNativeQueryRealizations().get(1).getSnapshots().size());
    }

    @Test
    public void testGetQueryHistoryWithMultiSnapshots() {
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        // set default values
        request.setStartTimeFrom("0");
        request.setStartTimeTo(String.valueOf(Long.MAX_VALUE));
        request.setLatencyFrom("0");
        request.setLatencyTo(String.valueOf(Integer.MAX_VALUE));

        // mock query histories
        QueryHistory snapshotQuery = new QueryHistory();
        snapshotQuery.setSql("select * from test_table_1");
        snapshotQuery.setEngineType("NATIVE");
        QueryMetrics.RealizationMetrics metrics1 = new QueryMetrics.RealizationMetrics(null, null,
                "741ca86a-1f13-46da-a59f-95fb68615e3a", Lists.newArrayList(new String[] {}));
        QueryMetrics.RealizationMetrics metrics2 = new QueryMetrics.RealizationMetrics("1", "Agg Index",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", Lists.newArrayList(new String[] { "snapshot1", "snapshot2" }));
        QueryMetrics.RealizationMetrics metrics3 = new QueryMetrics.RealizationMetrics("null", "null",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", Lists.newArrayList(new String[] { "snapshot1", "snapshot2" }));
        QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo();
        queryHistoryInfo.setRealizationMetrics(
                Lists.newArrayList(new QueryMetrics.RealizationMetrics[] { metrics1, metrics2, metrics3 }));
        snapshotQuery.setQueryHistoryInfo(queryHistoryInfo);

        RDBMSQueryHistoryDAO queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(snapshotQuery)).when(queryHistoryDAO)
                .getQueryHistoriesByConditions(Mockito.any(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(10L).when(queryHistoryDAO).getQueryHistoriesSize(Mockito.any(), Mockito.anyString());
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao();

        Map<String, Object> result = queryHistoryService.getQueryHistories(request, 10, 0);
        List<QueryHistory> queryHistories = (List<QueryHistory>) result.get("query_histories");
        Assert.assertEquals(1, queryHistories.size());
        Assert.assertEquals("nmodel_basic", queryHistories.get(0).getNativeQueryRealizations().get(1).getModelAlias());
        Assert.assertEquals(1L, (long) queryHistories.get(0).getNativeQueryRealizations().get(1).getLayoutId());
        Assert.assertEquals(true, queryHistories.get(0).getNativeQueryRealizations().get(0).getSnapshots().isEmpty());
        Assert.assertEquals(2, queryHistories.get(0).getNativeQueryRealizations().get(1).getSnapshots().size());
        Assert.assertNull(queryHistories.get(0).getNativeQueryRealizations().get(2).getLayoutId());
        Assert.assertNull(queryHistories.get(0).getNativeQueryRealizations().get(2).getIndexType());
    }

    @Test
    public void testcompatibilityBeforeKE_20697() {
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        // set default values
        request.setStartTimeFrom("0");
        request.setStartTimeTo(String.valueOf(Long.MAX_VALUE));
        request.setLatencyFrom("0");
        request.setLatencyTo(String.valueOf(Integer.MAX_VALUE));

        // mock query histories
        QueryHistory snapshotQuery = new QueryHistory();
        snapshotQuery.setSql("select * from test_table_1");
        snapshotQuery.setEngineType("NATIVE");
        snapshotQuery.setQueryRealizations(
                "741ca86a-1f13-46da-a59f-95fb68615e3a#null#null;89af4ee2-2cdb-4b07-b39e-4c29856309aa#1#Agg Index#[snapshot1, snapshot2]");

        RDBMSQueryHistoryDAO queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(snapshotQuery)).when(queryHistoryDAO)
                .getQueryHistoriesByConditions(Mockito.any(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(10L).when(queryHistoryDAO).getQueryHistoriesSize(Mockito.any(), Mockito.anyString());
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao();

        Map<String, Object> result = queryHistoryService.getQueryHistories(request, 10, 0);
        List<QueryHistory> queryHistories = (List<QueryHistory>) result.get("query_histories");
        Assert.assertEquals(1, queryHistories.size());
        Assert.assertEquals("nmodel_basic", queryHistories.get(0).getNativeQueryRealizations().get(1).getModelAlias());
        Assert.assertEquals(1L, (long) queryHistories.get(0).getNativeQueryRealizations().get(1).getLayoutId());
        Assert.assertEquals(true, queryHistories.get(0).getNativeQueryRealizations().get(0).getSnapshots().isEmpty());
        Assert.assertEquals(2, queryHistories.get(0).getNativeQueryRealizations().get(1).getSnapshots().size());
    }

    @Test
    public void testGetQueryHistoryFilter() {
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        // set default values
        request.setStartTimeFrom("0");
        request.setStartTimeTo(String.valueOf(Long.MAX_VALUE));
        request.setLatencyFrom("0");
        request.setLatencyTo(String.valueOf(Integer.MAX_VALUE));
        request.setRealizations(Lists.newArrayList("HIVE", "CONSTANTS", "nmodel_basic_inner", "nmodel_basic"));
        request.setSubmitterExactlyMatch(true);

        // mock query histories
        QueryHistory queryHistory = new QueryHistory();
        queryHistory.setSql("select * from test_table_1");
        queryHistory.setEngineType("NATIVE");
        queryHistory.setQuerySubmitter("TEST");
        QueryMetrics.RealizationMetrics noSnapshotMetrics1 = new QueryMetrics.RealizationMetrics("1", "Agg Index",
                "741ca86a-1f13-46da-a59f-95fb68615e3a", Lists.newArrayList(new String[] {}));
        QueryMetrics.RealizationMetrics noSnapshotMetrics2 = new QueryMetrics.RealizationMetrics("1", "Agg Index",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", Lists.newArrayList("test_snapshot"));
        QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo();
        queryHistoryInfo.setRealizationMetrics(Lists.newArrayList(noSnapshotMetrics1, noSnapshotMetrics2));
        queryHistory.setQueryHistoryInfo(queryHistoryInfo);

        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql("select * from test_table_3");
        queryHistory1.setEngineType("HIVE");
        queryHistory1.setQuerySubmitter("TEST");

        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setSql("select * from test_table_3");
        queryHistory2.setEngineType("CONSTANTS");
        queryHistory2.setQuerySubmitter("TEST");

        RDBMSQueryHistoryDAO queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(queryHistory, queryHistory1, queryHistory2)).when(queryHistoryDAO)
                .getQueryHistoriesByConditions(Mockito.any(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(10L).when(queryHistoryDAO).getQueryHistoriesSize(Mockito.any(), Mockito.anyString());
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao();

        Map<String, Object> result = queryHistoryService.getQueryHistories(request, 10, 0);
        List<QueryHistory> queryHistories = (List<QueryHistory>) result.get("query_histories");
        Assert.assertEquals(3, queryHistories.size());
        Assert.assertEquals("nmodel_basic", queryHistories.get(0).getNativeQueryRealizations().get(1).getModelAlias());
        Assert.assertEquals("HIVE", queryHistories.get(1).getEngineType());
        Assert.assertEquals("CONSTANTS", queryHistories.get(2).getEngineType());
        Assert.assertEquals("TEST", queryHistories.get(0).getQuerySubmitter());
        Assert.assertEquals("TEST", queryHistories.get(1).getQuerySubmitter());
        Assert.assertEquals("TEST", queryHistories.get(2).getQuerySubmitter());
    }

    @Test
    public void testGetQueryHistorySubmitters() {
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        // set default values
        request.setStartTimeFrom("0");
        request.setStartTimeTo(String.valueOf(Long.MAX_VALUE));
        request.setLatencyFrom("0");
        request.setLatencyTo(String.valueOf(Integer.MAX_VALUE));
        request.setRealizations(Lists.newArrayList("HIVE", "CONSTANTS", "nmodel_basic_inner", "nmodel_basic"));

        // mock query histories
        QueryHistory queryHistory = new QueryHistory();
        queryHistory.setQuerySubmitter("ADMIN");

        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setQuerySubmitter("TEST");

        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setQuerySubmitter("TEST2");

        RDBMSQueryHistoryDAO queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(queryHistory, queryHistory1, queryHistory2)).when(queryHistoryDAO)
                .getQueryHistoriesSubmitters(Mockito.any(), Mockito.anyInt());
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao();

        List<String> queryHistories = queryHistoryService.getQueryHistoryUsernames(request, 10);
        Assert.assertEquals(3, queryHistories.size());
        Assert.assertEquals("ADMIN", queryHistories.get(0));
        Assert.assertEquals("TEST", queryHistories.get(1));
        Assert.assertEquals("TEST2", queryHistories.get(2));
    }

    @Test
    public void testGetQueryHistoryModels() {
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        // set default values
        request.setStartTimeFrom("0");
        request.setStartTimeTo(String.valueOf(Long.MAX_VALUE));
        request.setLatencyFrom("0");
        request.setLatencyTo(String.valueOf(Integer.MAX_VALUE));
        request.setRealizations(Lists.newArrayList("HIVE", "CONSTANTS", "nmodel_basic_inner", "nmodel_basic"));

        // mock query query statistics

        QueryStatistics queryStatistics = new QueryStatistics();
        queryStatistics.setEngineType("HIVE");
        QueryStatistics queryStatistics1 = new QueryStatistics();
        queryStatistics1.setEngineType("CONSTANTS");
        QueryStatistics queryStatistics2 = new QueryStatistics();
        queryStatistics2.setModel("82fa7671-a935-45f5-8779-85703601f49a");

        QueryStatistics queryStatistics3 = new QueryStatistics();
        queryStatistics3.setModel("82fa7671-a935-45f5-8779-85703601f49b");

        RDBMSQueryHistoryDAO queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(queryStatistics, queryStatistics1, queryStatistics2, queryStatistics3))
                .when(queryHistoryDAO).getQueryHistoriesModelIds(Mockito.any(), Mockito.anyInt());
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao();

        List<String> models = queryHistoryService.getQueryHistoryModels(request, 10);
        Assert.assertEquals(3, models.size());
        Assert.assertEquals("HIVE", models.get(0));
        Assert.assertEquals("CONSTANTS", models.get(1));
        Assert.assertEquals("ut_inner_join_cube_partial", models.get(2));

    }

    @Test
    public void testQueryHistoryWithLayoutDel() {
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        QueryHistory queryHistory = new QueryHistory();
        queryHistory.setSql("select * from test_table_1");
        queryHistory.setQueryRealizations(
                "741ca86a-1f13-46da-a59f-95fb686153a#null#null,89af4ee2-2cdb-4b07-b39e-4c29856309aa#1222#Agg Index");

        RDBMSQueryHistoryDAO queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(queryHistory)).when(queryHistoryDAO)
                .getQueryHistoriesByConditions(Mockito.any(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(10L).when(queryHistoryDAO).getQueryHistoriesSize(Mockito.any(), Mockito.anyString());
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao();
        Map<String, Object> result = queryHistoryService.getQueryHistories(request, 10, 0);
        List<QueryHistory> queryHistories = (List<QueryHistory>) result.get("query_histories");

        Assert.assertFalse(queryHistories.get(0).getNativeQueryRealizations().get(0).isValid());
        Assert.assertFalse(queryHistories.get(0).getNativeQueryRealizations().get(1).isLayoutExist());
    }

    @Test
    public void testDownloadQueryHistories() throws Exception {
        // prepare query history to RDBMS
        RDBMSQueryHistoryDAO queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        queryHistoryDAO.deleteAllQueryHistory();
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311512000L, 1L, true, PROJECT, false));

        // prepare request and response
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream servletOutputStream = mock(ServletOutputStream.class);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        when(response.getOutputStream()).thenReturn(servletOutputStream);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                baos.write((byte[]) arguments[0]);
                return null;
            }
        }).when(servletOutputStream).write(any(byte[].class));

        queryHistoryService.downloadQueryHistories(request, response, ZoneOffset.ofHours(8), 8, false);
        assertEquals(
                "\uFEFFStart Time,Duration,Query ID,SQL Statement,Answered by,Query Status,Query Node,Submitter,Query Message\n"
                        + "2020-01-29 23:25:12 GMT+8,1ms,6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1,\"select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500\",CONSTANTS,SUCCEEDED,,ADMIN,\n"
                        + "2020-01-29 23:25:12 GMT+8,1ms,6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1,\"select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500\",\"[Deleted Model,Deleted Model]\",SUCCEEDED,,ADMIN,\n"
                        + "2020-01-29 23:25:12 GMT+8,1ms,6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1,\"select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500\",\"[Deleted Model,Deleted Model]\",SUCCEEDED,,ADMIN,\n",
                baos.toString(StandardCharsets.UTF_8.name()));
    }

    @Test
    public void testDownloadQueryHistoriesSql() throws Exception {
        // prepare query history to RDBMS
        RDBMSQueryHistoryDAO queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        queryHistoryDAO.deleteAllQueryHistory();
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));

        // prepare request and response
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream servletOutputStream = mock(ServletOutputStream.class);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        when(response.getOutputStream()).thenReturn(servletOutputStream);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                baos.write((byte[]) arguments[0]);
                return null;
            }
        }).when(servletOutputStream).write(any(byte[].class));

        queryHistoryService.downloadQueryHistories(request, response, null, null, true);
        assertEquals(
                "select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500;\n"
                        + "select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500;\n",
                baos.toString(StandardCharsets.UTF_8.name()));
    }

    @Test
    public void testDownloadQueryHistoriesSize() throws Exception {
        // prepare query history to RDBMS
        RDBMSQueryHistoryDAO queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        queryHistoryDAO.deleteAllQueryHistory();
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311512000L, 1L, true, PROJECT, false));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311522001L, 1L, true, PROJECT, true));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311532002L, 1L, true, PROJECT, true));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311542003L, 1L, true, PROJECT, false));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311552004L, 1L, true, PROJECT, false));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311562005L, 1L, true, PROJECT, false));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311572006L, 1L, true, PROJECT, false));

        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);

        overwriteSystemProp("kylin.query.query-history-download-batch-size", "2");
        overwriteSystemProp("kylin.query.query-history-download-max-size", "5");

        HttpServletResponse response1 = mock(HttpServletResponse.class);
        ServletOutputStream servletOutputStream1 = mock(ServletOutputStream.class);
        final ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
        when(response1.getOutputStream()).thenReturn(servletOutputStream1);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                baos1.write((byte[]) arguments[0]);
                return null;
            }
        }).when(servletOutputStream1).write(any(byte[].class));
        queryHistoryService.downloadQueryHistories(request, response1, ZoneOffset.ofHours(8), 8, false);
        assertEquals(
                "\uFEFFStart Time,Duration,Query ID,SQL Statement,Answered by,Query Status,Query Node,Submitter,Query Message\n"
                        + "2020-01-29 23:26:12 GMT+8,1ms,6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1,\"select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500\",CONSTANTS,SUCCEEDED,,ADMIN,\n"
                        + "2020-01-29 23:26:02 GMT+8,1ms,6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1,\"select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500\",CONSTANTS,SUCCEEDED,,ADMIN,\n"
                        + "2020-01-29 23:25:52 GMT+8,1ms,6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1,\"select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500\",CONSTANTS,SUCCEEDED,,ADMIN,\n"
                        + "2020-01-29 23:25:42 GMT+8,1ms,6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1,\"select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500\",CONSTANTS,SUCCEEDED,,ADMIN,\n"
                        + "2020-01-29 23:25:32 GMT+8,1ms,6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1,\"select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500\",\"[Deleted Model,Deleted Model]\",SUCCEEDED,,ADMIN,\n",
                baos1.toString(StandardCharsets.UTF_8.name()));

        overwriteSystemProp("kylin.query.query-history-download-batch-size", "3");
        overwriteSystemProp("kylin.query.query-history-download-max-size", "2");
        HttpServletResponse response2 = mock(HttpServletResponse.class);
        ServletOutputStream servletOutputStream2 = mock(ServletOutputStream.class);
        final ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        when(response2.getOutputStream()).thenReturn(servletOutputStream2);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                baos2.write((byte[]) arguments[0]);
                return null;
            }
        }).when(servletOutputStream2).write(any(byte[].class));

        queryHistoryService.downloadQueryHistories(request, response2, ZoneOffset.ofHours(8), 8, false);
        assertEquals(
                "\uFEFFStart Time,Duration,Query ID,SQL Statement,Answered by,Query Status,Query Node,Submitter,Query Message\n"
                        + "2020-01-29 23:26:12 GMT+8,1ms,6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1,\"select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500\",CONSTANTS,SUCCEEDED,,ADMIN,\n"
                        + "2020-01-29 23:26:02 GMT+8,1ms,6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1,\"select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500\",CONSTANTS,SUCCEEDED,,ADMIN,\n",
                baos2.toString(StandardCharsets.UTF_8.name()));
    }

    @Test
    public void testDownloadQueryHistoriesSqlSize() throws Exception {
        // prepare query history to RDBMS
        RDBMSQueryHistoryDAO queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        queryHistoryDAO.deleteAllQueryHistory();
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311512000L, 1L, true, PROJECT, false));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311522001L, 1L, true, PROJECT, true));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311532002L, 1L, true, PROJECT, true));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311542003L, 1L, true, PROJECT, false));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311552004L, 1L, true, PROJECT, false));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311562005L, 1L, true, PROJECT, false));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311572006L, 1L, true, PROJECT, false));

        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);

        overwriteSystemProp("kylin.query.query-history-download-batch-size", "2");
        overwriteSystemProp("kylin.query.query-history-download-max-size", "5");
        HttpServletResponse response1 = mock(HttpServletResponse.class);
        ServletOutputStream servletOutputStream1 = mock(ServletOutputStream.class);
        final ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
        when(response1.getOutputStream()).thenReturn(servletOutputStream1);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                baos1.write((byte[]) arguments[0]);
                return null;
            }
        }).when(servletOutputStream1).write(any(byte[].class));
        queryHistoryService.downloadQueryHistories(request, response1, ZoneOffset.ofHours(8), 8, true);
        assertEquals(
                "select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500;\n"
                        + "select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500;\n"
                        + "select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500;\n"
                        + "select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500;\n"
                        + "select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500;\n",
                baos1.toString(StandardCharsets.UTF_8.name()));

        overwriteSystemProp("kylin.query.query-history-download-batch-size", "2");
        overwriteSystemProp("kylin.query.query-history-download-max-size", "2");
        HttpServletResponse response2 = mock(HttpServletResponse.class);
        ServletOutputStream servletOutputStream2 = mock(ServletOutputStream.class);
        final ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        when(response2.getOutputStream()).thenReturn(servletOutputStream2);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                baos2.write((byte[]) arguments[0]);
                return null;
            }
        }).when(servletOutputStream2).write(any(byte[].class));

        queryHistoryService.downloadQueryHistories(request, response2, ZoneOffset.ofHours(8), 8, true);
        assertEquals(
                "select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500;\n"
                        + "select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500;\n",
                baos2.toString(StandardCharsets.UTF_8.name()));
    }

    @Test
    public void testGetFusionModelQueryHistories() {
        // when there is no filter conditions
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject("streaming_test");
        // set default values
        request.setStartTimeFrom("0");
        request.setStartTimeTo(String.valueOf(Long.MAX_VALUE));
        request.setLatencyFrom("0");
        request.setLatencyTo(String.valueOf(Integer.MAX_VALUE));

        // mock query histories

        QueryHistory query1 = new QueryHistory();
        query1.setSql("select * from test_table_1");

        QueryHistory query2 = new QueryHistory();
        query2.setSql("select * from test_table_2");

        QueryMetrics.RealizationMetrics metrics1 = new QueryMetrics.RealizationMetrics("1", "Agg Index",
                "b05034a8-c037-416b-aa26-9e6b4a41ee40", Lists.newArrayList(new String[] {}));
        QueryMetrics.RealizationMetrics metrics2 = new QueryMetrics.RealizationMetrics("1", "Agg Index",
                "334671fd-e383-4fc9-b5c2-94fce832f77a", Lists.newArrayList(new String[] {}));
        QueryMetrics.RealizationMetrics metrics3 = new QueryMetrics.RealizationMetrics("1", "Agg Index",
                "554671fd-e383-4fc9-b5c2-94fce832f77a", Lists.newArrayList(new String[] {}));

        QueryHistoryInfo queryHistoryInfo1 = new QueryHistoryInfo();
        queryHistoryInfo1.setRealizationMetrics(
                Lists.newArrayList(new QueryMetrics.RealizationMetrics[] { metrics1, metrics2 }));
        query1.setQueryHistoryInfo(queryHistoryInfo1);

        QueryHistoryInfo queryHistoryInfo2 = new QueryHistoryInfo();
        queryHistoryInfo2.setRealizationMetrics(Lists.newArrayList(new QueryMetrics.RealizationMetrics[] { metrics3 }));
        query2.setQueryHistoryInfo(queryHistoryInfo2);
        RDBMSQueryHistoryDAO queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(query1, query2)).when(queryHistoryDAO)
                .getQueryHistoriesByConditions(Mockito.any(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(10L).when(queryHistoryDAO).getQueryHistoriesSize(Mockito.any(), Mockito.anyString());
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao();

        Map<String, Object> result = queryHistoryService.getQueryHistories(request, 10, 0);
        List<QueryHistory> queryHistories = (List<QueryHistory>) result.get("query_histories");
        Assert.assertEquals("streaming_test",
                queryHistories.get(0).getNativeQueryRealizations().get(0).getModelAlias());
        Assert.assertEquals("streaming_test",
                queryHistories.get(0).getNativeQueryRealizations().get(1).getModelAlias());
        Assert.assertEquals("batch", queryHistories.get(1).getNativeQueryRealizations().get(0).getModelAlias());
    }

    @Test
    public void testGetQueryHistorySql() {
        QueryHistory qh = new QueryHistory();

        qh.setSql("select * from table1");
        QueryHistorySql qhs = qh.getQueryHistorySql();
        assertEquals("select * from table1", qhs.getSql());
        assertEquals("select * from table1", qhs.getNormalizedSql());

        qh.setSql("{\"sql\": \"select * from table1 -- comment\", \"normalized_sql\": \"select * from table1\"}");
        qhs = qh.getQueryHistorySql();
        assertEquals("select * from table1 -- comment", qhs.getSql());
        assertEquals("select * from table1", qhs.getNormalizedSql());
    }
}

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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.response.MetricsResponse;
import org.apache.kylin.engine.spark.utils.ComputedColumnEvalUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.util.ExpandableMeasureUtil;
import org.apache.kylin.metadata.query.QueryStatistics;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.JobStatisticsResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DashboardServiceTest extends SourceTestCase {

    public static final String MODEL = "model";
    public static final String DAY = "day";
    public static final String AVG_QUERY_LATENCY = "AVG_QUERY_LATENCY";
    public static final String JOB = "JOB";
    public static final String AVG_JOB_BUILD_TIME = "AVG_JOB_BUILD_TIME";
    private static final String WEEK = "week";
    private static final String MONTH = "month";
    private static final String QUERY = "QUERY";
    private static final String QUERY_COUNT = "QUERY_COUNT";
    private static final String JOB_COUNT = "JOB_COUNT";
    @InjectMocks
    private final DashboardService dashboardService = Mockito.spy(new DashboardService());
    @InjectMocks
    private final ModelService modelService = Mockito.spy(new ModelService());
    @InjectMocks
    private final JobService jobService = Mockito.spy(new JobService());
    @InjectMocks
    private final QueryHistoryService queryHistoryService = Mockito.spy(new QueryHistoryService());
    @InjectMocks
    private final ModelBuildService modelBuildService = Mockito.spy(new ModelBuildService());
    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());
    @InjectMocks
    private final ProjectService projectService = Mockito.spy(new ProjectService());
    @InjectMocks
    private final MockModelQueryService modelQueryService = Mockito.spy(new MockModelQueryService());

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);
    @Mock
    private final AccessService accessService = Mockito.spy(AccessService.class);
    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Before
    public void setup() {
        super.setUp();
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "accessService", accessService);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(semanticService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelBuildService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(semanticService, "expandableMeasureUtil",
                new ExpandableMeasureUtil((model, ccDesc) -> {
                    String ccExpression = PushDownUtil.massageComputedColumn(model, model.getProject(), ccDesc,
                            AclPermissionUtil.createAclInfo(model.getProject(),
                                    semanticService.getCurrentUserGroups()));
                    ccDesc.setInnerExpression(ccExpression);
                    ComputedColumnEvalUtil.evaluateExprAndType(model, ccDesc);
                }));
        ReflectionTestUtils.setField(modelService, "projectService", projectService);
        ReflectionTestUtils.setField(modelService, "modelQuerySupporter", modelQueryService);
        ReflectionTestUtils.setField(modelService, "modelBuildService", modelBuildService);

        ReflectionTestUtils.setField(modelBuildService, "modelService", modelService);
        ReflectionTestUtils.setField(modelBuildService, "aclEvaluate", aclEvaluate);
        modelService.setSemanticUpdater(semanticService);

        ReflectionTestUtils.setField(jobService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(jobService, "projectService", projectService);
        ReflectionTestUtils.setField(jobService, "modelService", modelService);

        ReflectionTestUtils.setField(queryHistoryService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(queryHistoryService, "modelService", modelService);
        ReflectionTestUtils.setField(queryHistoryService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(queryHistoryService, "asyncTaskService", new AsyncTaskService());

        ReflectionTestUtils.setField(dashboardService, "jobService", jobService);
        ReflectionTestUtils.setField(dashboardService, "modelService", modelService);
        ReflectionTestUtils.setField(dashboardService, "queryHistoryService", queryHistoryService);

        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
    }

    @Test
    public void testGetModelMetrics() {
        MetricsResponse modelMetrics = dashboardService.getModelMetrics(getProject(), null);
        Assert.assertEquals(4, modelMetrics.size());
    }

    @Test
    public void testGetQueryMetrics() {
        QueryStatistics queryStatistics = new QueryStatistics();
        queryStatistics.setCount(777);
        queryStatistics.setMeanDuration(7070);

        RDBMSQueryHistoryDAO queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        Mockito.doReturn(queryStatistics).when(queryHistoryDAO).getQueryCountAndAvgDurationRealization(1262275200000L,
                1640966400000L, "default");
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao();

        MetricsResponse queryMetrics = dashboardService.getQueryMetrics(getProject(), "2010-01-01", "2022-01-01");
        Assert.assertEquals(2, queryMetrics.size());
        Assert.assertEquals(777, (double) queryMetrics.get("queryCount"), 0.1);
        Assert.assertEquals(7070, (double) queryMetrics.get("avgQueryLatency"), 0.1);
    }

    @Test
    public void testGetJobMetrics() {
        JobStatisticsResponse jobStats = jobService.getJobStats("default", Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(0, jobStats.getCount());
        Assert.assertEquals(0, jobStats.getTotalByteSize());
        Assert.assertEquals(0, jobStats.getTotalDuration());

        String startTime = "2018-01-01";
        String endTime = "2018-02-01";

        MetricsResponse metricsResponse = dashboardService.getJobMetrics(getProject(), startTime, endTime);
        Assert.assertEquals(3, metricsResponse.size());
        Assert.assertEquals((float) 0, metricsResponse.get("jobCount"), 0.1);
    }

    @Test
    public void testGetChartDataOfQuery() throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault(Locale.Category.FORMAT));
        String _startTime = "2018-01-01";
        String _endTime = "2018-01-03";

        long startTime = format.parse("2018-01-01").getTime();
        long endTime = format.parse("2018-01-03").getTime();

        RDBMSQueryHistoryDAO queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        Mockito.doReturn(getTestStatistics()).when(queryHistoryDAO).getQueryCountByModel(startTime, endTime, "default");
        Mockito.doReturn(getTestStatistics()).when(queryHistoryDAO).getQueryCountRealizationByTime(Mockito.anyLong(),
                Mockito.anyLong(), Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(getTestStatistics()).when(queryHistoryDAO).getAvgDurationByModel(startTime, endTime,
                "default");
        Mockito.doReturn(getTestStatistics()).when(queryHistoryDAO).getAvgDurationRealizationByTime(Mockito.anyLong(),
                Mockito.anyLong(), Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao();

        // QUERY_COUNT
        // query count by model
        MetricsResponse metricsResponse = dashboardService.getChartData(QUERY, getProject(), _startTime, _endTime,
                MODEL, QUERY_COUNT);
        Assert.assertEquals(3, metricsResponse.size());
        Assert.assertEquals(10, (double) metricsResponse.get("nmodel_basic"), 0.1);
        Assert.assertEquals(11, (double) metricsResponse.get("all_fixed_length"), 0.1);

        // query count by day
        metricsResponse = dashboardService.getChartData(QUERY, getProject(), _startTime, _endTime, DAY, QUERY_COUNT);
        Assert.assertEquals(4, metricsResponse.size());
        Assert.assertEquals(10, (double) metricsResponse.get("2018-01-01"), 0.1);
        Assert.assertEquals(11, (double) metricsResponse.get("2018-01-02"), 0.1);

        // query count by week
        metricsResponse = dashboardService.getChartData(QUERY, getProject(), _startTime, _endTime, WEEK, QUERY_COUNT);
        Assert.assertEquals(5, metricsResponse.size());
        Assert.assertEquals(10, (double) metricsResponse.get("2018-01-01"), 0.1);
        Assert.assertEquals(11, (double) metricsResponse.get("2018-01-02"), 0.1);

        // query count by month
        metricsResponse = dashboardService.getChartData(QUERY, getProject(), _startTime, _endTime, MONTH, QUERY_COUNT);
        Assert.assertEquals(2, metricsResponse.size());
        Assert.assertEquals(11, (double) metricsResponse.get("2018-01"), 0.1);

        Mockito.doReturn(getTestStatistics()).when(queryHistoryDAO).getAvgDurationByModel(startTime, endTime,
                "default");
        Mockito.doReturn(getTestStatistics()).when(queryHistoryDAO).getAvgDurationByTime(Mockito.anyLong(),
                Mockito.anyLong(), Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao();

        // AVG_QUERY_LATENCY
        // avg duration by model
        metricsResponse = dashboardService.getChartData(QUERY, getProject(), _startTime, _endTime, MODEL,
                AVG_QUERY_LATENCY);
        Assert.assertEquals(3, metricsResponse.size());
        Assert.assertEquals(500, (double) metricsResponse.get("nmodel_basic"), 0.1);
        Assert.assertEquals(600, (double) metricsResponse.get("all_fixed_length"), 0.1);

        // avg duration by day
        metricsResponse = dashboardService.getChartData(QUERY, getProject(), _startTime, _endTime, DAY,
                AVG_QUERY_LATENCY);
        Assert.assertEquals(4, metricsResponse.size());
        Assert.assertEquals(500, (double) metricsResponse.get("2018-01-01"), 0.1);
        Assert.assertEquals(600, (double) metricsResponse.get("2018-01-02"), 0.1);

        // avg duration by week
        metricsResponse = dashboardService.getChartData(QUERY, getProject(), _startTime, _endTime, WEEK,
                AVG_QUERY_LATENCY);
        Assert.assertEquals(5, metricsResponse.size());
        Assert.assertEquals(500, (double) metricsResponse.get("2018-01-01"), 0.1);
        Assert.assertEquals(600, (double) metricsResponse.get("2018-01-02"), 0.1);

        // avg duration by month
        metricsResponse = dashboardService.getChartData(QUERY, getProject(), _startTime, _endTime, MONTH,
                AVG_QUERY_LATENCY);
        Assert.assertEquals(2, metricsResponse.size());
        Assert.assertEquals(600, (double) metricsResponse.get("2018-01"), 0.1);
    }

    @Test
    public void testGetChartDataOfJob() {
        JobStatisticsResponse jobStats = jobService.getJobStats("default", Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(0, jobStats.getCount());
        Assert.assertEquals(0, jobStats.getTotalByteSize());
        Assert.assertEquals(0, jobStats.getTotalDuration());

        String startTime = "2018-01-01";
        String endTime = "2018-02-01";

        //JOB_COUNT
        //model
        MetricsResponse metricsResponse = dashboardService.getChartData(JOB, getProject(), startTime, endTime, MODEL,
                JOB_COUNT);
        Assert.assertEquals(0, metricsResponse.size());

        //day
        metricsResponse = dashboardService.getChartData(JOB, getProject(), startTime, endTime, DAY, JOB_COUNT);
        Assert.assertEquals(32, metricsResponse.size());
        Assert.assertEquals(0, (double) metricsResponse.get("2018-01-01"), 0.1);
        Assert.assertEquals(0, (double) metricsResponse.get("2018-02-01"), 0.1);

        //week
        metricsResponse = dashboardService.getChartData(JOB, getProject(), startTime, endTime, WEEK, JOB_COUNT);
        Assert.assertEquals(5, metricsResponse.size());
        Assert.assertEquals(0, (double) metricsResponse.get("2018-01-08"), 0.1);
        Assert.assertEquals(0, (double) metricsResponse.get("2018-01-29"), 0.1);

        //month
        metricsResponse = dashboardService.getChartData(JOB, getProject(), startTime, endTime, MONTH, JOB_COUNT);
        Assert.assertEquals(2, metricsResponse.size());
        Assert.assertEquals(0, (double) metricsResponse.get("2018-01-01"), 0.1);
        Assert.assertEquals(0, (double) metricsResponse.get("2018-02-01"), 0.1);

        //AVG_BUILD_TIME
        //model
        metricsResponse = dashboardService.getChartData(JOB, getProject(), startTime, endTime, MODEL,
                AVG_JOB_BUILD_TIME);
        Assert.assertEquals(0, metricsResponse.size());

        //day
        metricsResponse = dashboardService.getChartData(JOB, getProject(), startTime, endTime, DAY, AVG_JOB_BUILD_TIME);
        Assert.assertEquals(32, metricsResponse.size());
        Assert.assertEquals(0, (double) metricsResponse.get("2018-01-01"), 0.1);
        Assert.assertEquals(0, (double) metricsResponse.get("2018-02-01"), 0.1);

        //week
        metricsResponse = dashboardService.getChartData(JOB, getProject(), startTime, endTime, WEEK,
                AVG_JOB_BUILD_TIME);
        Assert.assertEquals(5, metricsResponse.size());
        Assert.assertEquals(0, (double) metricsResponse.get("2018-01-08"), 0.1);
        Assert.assertEquals(0, (double) metricsResponse.get("2018-01-29"), 0.1);

        //month
        metricsResponse = dashboardService.getChartData(JOB, getProject(), startTime, endTime, MONTH,
                AVG_JOB_BUILD_TIME);
        Assert.assertEquals(2, metricsResponse.size());
        Assert.assertEquals(0, (double) metricsResponse.get("2018-01-01"), 0.1);
        Assert.assertEquals(0, (double) metricsResponse.get("2018-02-01"), 0.1);
    }

    @Test
    public void testErrorCase() {
        String errorMsg = "";
        try {
            dashboardService.getChartData("error", getProject(), "2018-01-01", "2018-02-01", null, null);
        } catch (Exception e) {
            errorMsg = e.getMessage();
        }
        Assert.assertNotNull(errorMsg);

        errorMsg = "";
        try {
            dashboardService.getChartData(QUERY, getProject(), "2018-01-01", "2018-02-01", null, "error");
        } catch (Exception e) {
            errorMsg = e.getMessage();
        }
        Assert.assertNotNull(errorMsg);

        errorMsg = "";
        try {
            dashboardService.getChartData(JOB, getProject(), "2018-01-01", "2018-02-01", null, "error");
        } catch (Exception e) {
            errorMsg = e.getMessage();
        }
        Assert.assertNotNull(errorMsg);

    }

    @Test
    public void testCheckAuthorization() {
        dashboardService.checkAuthorization(null);
        dashboardService.checkAuthorization(getProject());
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
}

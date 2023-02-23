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

package org.apache.kylin.metadata.query;

import static org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO.fillZeroForQueryStatistics;
import static org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO.largeSplitToSmallTask;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.junit.TimeZoneTestRunner;
import org.apache.kylin.metadata.query.util.QueryHisStoreUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Lists;

@RunWith(TimeZoneTestRunner.class)
public class RDBMSQueryHistoryDaoTest extends NLocalFileMetadataTestCase {

    String PROJECT = "default";
    public static final String WEEK = "week";
    public static final String DAY = "day";
    public static final String NORMAL_USER = "normal_user";
    public static final String ADMIN = "ADMIN";

    private RDBMSQueryHistoryDAO queryHistoryDAO;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
    }

    @After
    public void destroy() throws Exception {
        queryHistoryDAO.deleteAllQueryHistory();
        cleanupTestMetadata();
    }

    @Test
    public void testInsert() {
        List<QueryMetrics> queryMetricsList = Lists.newArrayList();
        for (int i = 0; i < 100; i++) {
            queryMetricsList.add(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        }
        queryHistoryDAO.insert(queryMetricsList);
        List<QueryHistory> queryHistoryList = queryHistoryDAO.queryQueryHistoriesByIdOffset(0, 200, "default");
        Assert.assertEquals(100, queryHistoryList.size());
    }

    @Test
    public void testGetQueryHistoriesFilterByIsIndexHit() throws Exception {
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, false, PROJECT, true));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, false, PROJECT, true));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, false, "otherProject", true));

        // filter all
        QueryHistoryRequest queryHistoryRequest = new QueryHistoryRequest();
        queryHistoryRequest.setProject(PROJECT);
        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(ADMIN);
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(3, queryHistoryList.size());

        // filter hit index
        queryHistoryRequest.setRealizations(Lists.newArrayList("modelName"));
        queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(1, queryHistoryList.size());

        // filter not hit index
        queryHistoryRequest.setRealizations(Lists.newArrayList("pushdown"));
        queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(2, queryHistoryList.size());

        // filter all
        queryHistoryRequest.setRealizations(Lists.newArrayList("modelName", "pushdown"));
        queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(3, queryHistoryList.size());
    }

    @Test
    public void testGetQueryHistoriesFilterByQueryTime() throws Exception {
        // 2020-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        // 2020-01-30 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 1L, false, PROJECT, true));
        // 2020-01-31 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580484312000L, 1L, false, PROJECT, true));
        // 2021-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1611933912000L, 1L, false, PROJECT, true));

        QueryHistoryRequest queryHistoryRequest = new QueryHistoryRequest();
        queryHistoryRequest.setProject(PROJECT);
        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(ADMIN);
        queryHistoryRequest.setStartTimeFrom("1580397912000");
        queryHistoryRequest.setStartTimeTo("1580484312000");
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(1, queryHistoryList.size());
    }

    @Test
    public void testGetQueryHistoriesFilterByDuration() throws Exception {
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1000L, true, PROJECT, true));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 2000L, false, PROJECT, true));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 3000L, false, PROJECT, true));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 4000L, false, PROJECT, true));

        QueryHistoryRequest queryHistoryRequest = new QueryHistoryRequest();
        queryHistoryRequest.setProject(PROJECT);
        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(ADMIN);
        queryHistoryRequest.setLatencyFrom("1");
        queryHistoryRequest.setLatencyTo("4");
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(3, queryHistoryList.size());

        queryHistoryRequest.setLatencyFrom("2");
        queryHistoryRequest.setLatencyTo("3");
        queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(1, queryHistoryList.size());
    }

    @Test
    public void testGetQueryHistoriesFilterBySql() throws Exception {
        QueryMetrics queryMetrics1 = createQueryMetrics(1580311512000L, 1L, true, PROJECT, true);
        queryMetrics1.setSql("select 2 LIMIT 500\n");
        queryHistoryDAO.insert(queryMetrics1);

        QueryMetrics queryMetrics2 = createQueryMetrics(1580311512000L, 1L, true, PROJECT, true);
        queryMetrics2.setSql("select 1 LIMIT 500\n");
        queryHistoryDAO.insert(queryMetrics2);

        QueryMetrics queryMetrics3 = createQueryMetrics(1580311512000L, 1L, true, PROJECT, true);
        queryMetrics3.setSql("select count(*) from KYLIN_SALES group by BUYER_ID LIMIT 500");
        queryHistoryDAO.insert(queryMetrics3);

        QueryMetrics queryMetrics4 = createQueryMetrics(1580311512000L, 1L, true, PROJECT, true);
        queryMetrics4.setSql("select count(*) from KYLIN_SALES");
        queryHistoryDAO.insert(queryMetrics4);

        QueryHistoryRequest queryHistoryRequest = new QueryHistoryRequest();
        queryHistoryRequest.setProject(PROJECT);
        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(ADMIN);

        queryHistoryRequest.setSql("count");
        List<QueryHistory> queryHistoryList1 = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10,
                0);
        Assert.assertEquals(2, queryHistoryList1.size());

        queryHistoryRequest.setSql("LIMIT");
        List<QueryHistory> queryHistoryList2 = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10,
                0);
        Assert.assertEquals(3, queryHistoryList2.size());

        queryHistoryRequest.setSql("select 1");
        List<QueryHistory> queryHistoryList3 = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10,
                0);
        Assert.assertEquals(1, queryHistoryList3.size());

        queryHistoryRequest.setSql("6a9a151f");
        List<QueryHistory> queryHistoryList4 = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10,
                0);
        Assert.assertEquals(4, queryHistoryList4.size());

        for (int i = 0; i < 30; i++) {
            queryHistoryDAO.insert(queryMetrics1);
        }
        Assert.assertEquals(10, queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0).size());
        Assert.assertEquals(10, queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 1).size());
        Assert.assertEquals(10, queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 2).size());
        Assert.assertEquals(4, queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 3).size());
        Assert.assertEquals(20, queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 20, 0).size());
        Assert.assertEquals(14, queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 20, 1).size());
    }

    @Test
    public void getQueryHistoriesById() {
        Assert.assertEquals(1, queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true)));
        Assert.assertEquals(1, queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true)));
        List<QueryHistory> queryHistoryList = queryHistoryDAO.queryQueryHistoriesByIdOffset(0, 10, "default");
        Assert.assertEquals(2, queryHistoryList.size());
        Assert.assertEquals("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", queryHistoryList.get(0).getQueryId());
        Assert.assertNotNull(queryHistoryList.get(0).getQueryHistoryInfo());
    }

    @Test
    public void testGetQueryHistoriesSize() throws Exception {
        QueryHistoryRequest queryHistoryRequest = new QueryHistoryRequest();
        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(ADMIN);
        queryHistoryRequest.setProject(PROJECT);

        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        long queryHistoriesSize = queryHistoryDAO.getQueryHistoriesSize(queryHistoryRequest, PROJECT);
        Assert.assertEquals(2, queryHistoriesSize);

        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        queryHistoriesSize = queryHistoryDAO.getQueryHistoriesSize(queryHistoryRequest, PROJECT);
        Assert.assertEquals(5, queryHistoriesSize);
    }

    @Test
    public void testGetQueryCountByTime() throws Exception {
        // 2020-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        // 2020-01-30 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 1L, false, PROJECT, true));
        // 2020-01-31 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580484312000L, 1L, false, PROJECT, true));
        // 2021-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1611933912000L, 1L, false, PROJECT, true));

        // filter from 2020-01-26 23:25:11 to 2020-01-31 23:25:13
        List<QueryStatistics> dayQueryStatistics = queryHistoryDAO.getQueryCountByTime(1580052311000L, 1580484313000L,
                "day", PROJECT);
        Assert.assertEquals(3, dayQueryStatistics.size());
        Assert.assertEquals("2020-01-31T00:00:00Z", dayQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(1, dayQueryStatistics.get(0).getCount());
        Assert.assertEquals("2020-01-29T00:00:00Z", dayQueryStatistics.get(1).getTime().toString());
        Assert.assertEquals(1, dayQueryStatistics.get(1).getCount());
        Assert.assertEquals("2020-01-30T00:00:00Z", dayQueryStatistics.get(2).getTime().toString());
        Assert.assertEquals(1, dayQueryStatistics.get(2).getCount());
        fillZeroForQueryStatistics(dayQueryStatistics, 1580052311000L, 1580484313000L, DAY);
        Assert.assertEquals("2020-01-31T00:00:00Z", dayQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(1, dayQueryStatistics.get(0).getCount());
        Assert.assertEquals("2020-01-29T00:00:00Z", dayQueryStatistics.get(1).getTime().toString());
        Assert.assertEquals(1, dayQueryStatistics.get(1).getCount());
        Assert.assertEquals("2020-01-30T00:00:00Z", dayQueryStatistics.get(2).getTime().toString());
        Assert.assertEquals(1, dayQueryStatistics.get(2).getCount());
        Assert.assertEquals("2020-01-26T00:00:00Z", dayQueryStatistics.get(3).getTime().toString());
        Assert.assertEquals(0, dayQueryStatistics.get(3).getCount());
        Assert.assertEquals("2020-01-27T00:00:00Z", dayQueryStatistics.get(4).getTime().toString());
        Assert.assertEquals(0, dayQueryStatistics.get(4).getCount());
        Assert.assertEquals("2020-01-28T00:00:00Z", dayQueryStatistics.get(5).getTime().toString());
        Assert.assertEquals(0, dayQueryStatistics.get(5).getCount());

        List<QueryStatistics> weekQueryStatistics = queryHistoryDAO.getQueryCountByTime(1580052311000L, 1580484313000L,
                "week", PROJECT);
        Assert.assertEquals(1, weekQueryStatistics.size());
        Assert.assertEquals("2020-01-26T00:00:00Z", weekQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(3, weekQueryStatistics.get(0).getCount());
        fillZeroForQueryStatistics(weekQueryStatistics, 1580052311000L, 1580484313000L, WEEK);
        Assert.assertEquals(1, weekQueryStatistics.size());
        Assert.assertEquals("2020-01-26T00:00:00Z", weekQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(3, weekQueryStatistics.get(0).getCount());

        List<QueryStatistics> monthQueryStatistics = queryHistoryDAO.getQueryCountByTime(1580052311000L, 1580484313000L,
                "month", PROJECT);
        Assert.assertEquals(1, monthQueryStatistics.size());
        Assert.assertEquals(3, monthQueryStatistics.get(0).getCount());
        fillZeroForQueryStatistics(monthQueryStatistics, 1580052311000L, 1580484313000L, "month");
        Assert.assertEquals(3, monthQueryStatistics.get(0).getCount());
    }

    @Test
    public void testGetAvgDurationByTime() throws Exception {
        // 2020-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        // 2020-01-30 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 2L, false, PROJECT, true));
        // 2020-01-31 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580484312000L, 3L, false, PROJECT, true));
        // 2021-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1611933912000L, 1L, false, PROJECT, true));

        // filter from 2020-01-26 23:25:11 to 2020-01-31 23:25:13
        List<QueryStatistics> dayQueryStatistics = queryHistoryDAO.getAvgDurationByTime(1580052311000L, 1580484313000L,
                "day", PROJECT);
        Assert.assertEquals(3, dayQueryStatistics.size());
        Assert.assertEquals("2020-01-31T00:00:00Z", dayQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(3, dayQueryStatistics.get(0).getMeanDuration(), 0.1);
        Assert.assertEquals("2020-01-29T00:00:00Z", dayQueryStatistics.get(1).getTime().toString());
        Assert.assertEquals(1, dayQueryStatistics.get(1).getMeanDuration(), 0.1);
        Assert.assertEquals("2020-01-30T00:00:00Z", dayQueryStatistics.get(2).getTime().toString());
        Assert.assertEquals(2, dayQueryStatistics.get(2).getMeanDuration(), 0.1);
        fillZeroForQueryStatistics(dayQueryStatistics, 1580052311000L, 1580484313000L, DAY);
        Assert.assertEquals("2020-01-31T00:00:00Z", dayQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(3, dayQueryStatistics.get(0).getMeanDuration(), 0.1);
        Assert.assertEquals("2020-01-29T00:00:00Z", dayQueryStatistics.get(1).getTime().toString());
        Assert.assertEquals(1, dayQueryStatistics.get(1).getMeanDuration(), 0.1);
        Assert.assertEquals("2020-01-30T00:00:00Z", dayQueryStatistics.get(2).getTime().toString());
        Assert.assertEquals(2, dayQueryStatistics.get(2).getMeanDuration(), 0.1);
        Assert.assertEquals("2020-01-26T00:00:00Z", dayQueryStatistics.get(3).getTime().toString());
        Assert.assertEquals(0, dayQueryStatistics.get(3).getMeanDuration(), 0.1);
        Assert.assertEquals("2020-01-27T00:00:00Z", dayQueryStatistics.get(4).getTime().toString());
        Assert.assertEquals(0, dayQueryStatistics.get(4).getMeanDuration(), 0.1);
        Assert.assertEquals("2020-01-28T00:00:00Z", dayQueryStatistics.get(5).getTime().toString());
        Assert.assertEquals(0, dayQueryStatistics.get(5).getMeanDuration(), 0.1);

        List<QueryStatistics> weekQueryStatistics = queryHistoryDAO.getAvgDurationByTime(1580052311000L, 1580484313000L,
                "week", PROJECT);
        Assert.assertEquals(1, weekQueryStatistics.size());
        Assert.assertEquals("2020-01-26T00:00:00Z", weekQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(2, weekQueryStatistics.get(0).getMeanDuration(), 0.1);
        fillZeroForQueryStatistics(weekQueryStatistics, 1580052311000L, 1580484313000L, WEEK);
        Assert.assertEquals(1, weekQueryStatistics.size());
        Assert.assertEquals("2020-01-26T00:00:00Z", weekQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(2, weekQueryStatistics.get(0).getMeanDuration(), 0.1);

        List<QueryStatistics> monthQueryStatistics = queryHistoryDAO.getAvgDurationByTime(1580052311000L,
                1580484313000L, "month", PROJECT);
        Assert.assertEquals(1, monthQueryStatistics.size());
        Assert.assertEquals(2, monthQueryStatistics.get(0).getMeanDuration(), 0.1);
        fillZeroForQueryStatistics(monthQueryStatistics, 1580052311000L, 1580484313000L, "month");
        Assert.assertEquals(2, monthQueryStatistics.get(0).getMeanDuration(), 0.1);
    }

    @Test
    public void testDeleteQueryHistories() throws Exception {
        overwriteSystemProp("kylin.query.queryhistory.max-size", "2");
        overwriteSystemProp("kylin.query.queryhistory.project-max-size", "5");

        String PROJECT_V1 = PROJECT + "_v1";

        // 2020-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        // 2020-01-30 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 2L, false, PROJECT, true));
        // 2030-01-28 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1895844312000L, 3L, false, PROJECT_V1, true));
        // 2030-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1895930712000L, 1L, false, PROJECT, true));

        // before delete
        List<QueryHistory> queryHistoryList = queryHistoryDAO.queryQueryHistoriesByIdOffset(0, 100, PROJECT);
        Assert.assertEquals(3, queryHistoryList.size());

        // after delete
        QueryHisStoreUtil.cleanQueryHistory();

        queryHistoryList = queryHistoryDAO.queryQueryHistoriesByIdOffset(0, 100, PROJECT_V1);
        Assert.assertEquals(1, queryHistoryList.size());
        Assert.assertEquals(1895844312000L, queryHistoryList.get(0).getQueryTime());

        queryHistoryList = queryHistoryDAO.queryQueryHistoriesByIdOffset(0, 100, PROJECT);
        Assert.assertEquals(1, queryHistoryList.size());
        Assert.assertEquals(1895930712000L, queryHistoryList.get(0).getQueryTime());
    }

    @Test
    public void testDeleteQueryHistoriesIfRetainTimeReached() throws Exception {
        // 2020-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        // 2020-01-30 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 2L, false, PROJECT, true));
        // 2020-01-31 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580484312000L, 3L, false, PROJECT, true));
        // 2030-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1895930712000L, 1L, false, PROJECT, true));

        // before delete
        List<QueryHistory> queryHistoryList = queryHistoryDAO.queryQueryHistoriesByIdOffset(0, 100, PROJECT);
        Assert.assertEquals(4, queryHistoryList.size());

        // after delete
        queryHistoryDAO.deleteQueryHistoriesIfRetainTimeReached();
        queryHistoryList = queryHistoryDAO.queryQueryHistoriesByIdOffset(0, 100, PROJECT);
        Assert.assertEquals(1, queryHistoryList.size());
        Assert.assertEquals(1895930712000L, queryHistoryList.get(0).getQueryTime());
    }

    @Test
    public void testDeleteQueryHistoriesIfMaxSizeReached() throws Exception {
        overwriteSystemProp("kylin.query.queryhistory.max-size", "2");

        // 2020-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        // 2020-01-30 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 2L, false, PROJECT, true));
        // 2020-01-31 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580484312000L, 3L, false, PROJECT, true));
        // 2030-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1895930712000L, 1L, false, PROJECT, true));

        // before delete
        List<QueryHistory> queryHistoryList = queryHistoryDAO.queryQueryHistoriesByIdOffset(0, 100, PROJECT);
        Assert.assertEquals(4, queryHistoryList.size());

        // after delete
        queryHistoryDAO.deleteQueryHistoriesIfMaxSizeReached();
        queryHistoryList = queryHistoryDAO.queryQueryHistoriesByIdOffset(0, 100, PROJECT);
        Assert.assertEquals(2, queryHistoryList.size());

        // test delete empty
        queryHistoryDAO.deleteQueryHistoriesIfMaxSizeReached();
        queryHistoryList = queryHistoryDAO.queryQueryHistoriesByIdOffset(0, 100, PROJECT);
        Assert.assertEquals(2, queryHistoryList.size());
    }

    @Test
    public void testDeleteQueryHistoriesIfProjectMaxSizeReached() throws Exception {
        overwriteSystemProp("kylin.query.queryhistory.project-max-size", "2");

        // 2020-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        // 2020-01-30 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 2L, false, PROJECT, true));
        // 2020-01-31 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580484312000L, 3L, false, PROJECT, true));
        // 2030-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1895930712000L, 1L, false, PROJECT, true));

        // before delete
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getAllQueryHistories();
        Assert.assertEquals(4, queryHistoryList.size());

        // after delete
        QueryHisStoreUtil.cleanQueryHistory(PROJECT, 4);
        queryHistoryList = queryHistoryDAO.getAllQueryHistories();
        Assert.assertEquals(2, queryHistoryList.size());

        // test delete empty
        QueryHisStoreUtil.cleanQueryHistory(PROJECT, 2);
        queryHistoryList = queryHistoryDAO.getAllQueryHistories();
        Assert.assertEquals(2, queryHistoryList.size());
    }

    @Test
    public void testDropProjectMeasurement() throws Exception {
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 2L, false, PROJECT, true));
        queryHistoryDAO.insert(createQueryMetrics(1580484312000L, 3L, false, PROJECT, true));
        queryHistoryDAO.insert(createQueryMetrics(1895930712000L, 1L, false, "other", true));

        // before delete
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getAllQueryHistories();
        Assert.assertEquals(4, queryHistoryList.size());

        // after delete
        queryHistoryDAO.dropProjectMeasurement(PROJECT);
        queryHistoryList = queryHistoryDAO.getAllQueryHistories();
        Assert.assertEquals(1, queryHistoryList.size());
        Assert.assertEquals("other", queryHistoryList.get(0).getProjectName());
    }

    @Test
    public void testDeleteQueryHistoryForProject() throws Exception {
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT, true));
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 2L, false, PROJECT, true));
        queryHistoryDAO.insert(createQueryMetrics(1580484312000L, 3L, false, PROJECT, true));
        queryHistoryDAO.insert(createQueryMetrics(1895930712000L, 1L, false, "other", true));

        // before delete
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getAllQueryHistories();
        Assert.assertEquals(4, queryHistoryList.size());

        // after delete
        queryHistoryDAO.deleteQueryHistoryByProject(PROJECT);
        queryHistoryList = queryHistoryDAO.getAllQueryHistories();
        Assert.assertEquals(1, queryHistoryList.size());
        Assert.assertEquals("other", queryHistoryList.get(0).getProjectName());
    }

    @Test
    public void testUpdateQueryHistoryInfo() throws Exception {
        QueryMetrics queryMetrics1 = createQueryMetrics(1580311512000L, 1L, true, PROJECT, true);
        QueryMetrics queryMetrics2 = createQueryMetrics(1580397912000L, 2L, false, PROJECT, true);
        QueryMetrics queryMetrics3 = createQueryMetrics(1580484312000L, 3L, false, PROJECT, true);
        QueryMetrics queryMetrics4 = createQueryMetrics(1895930712000L, 1L, false, "other", true);
        queryHistoryDAO.insert(queryMetrics1);
        queryHistoryDAO.insert(queryMetrics2);
        queryHistoryDAO.insert(queryMetrics3);
        queryHistoryDAO.insert(queryMetrics4);

        List<Pair<Long, QueryHistoryInfo>> qhInfoList = Lists.newArrayList();
        QueryHistoryInfo queryHistoryInfo1 = new QueryHistoryInfo(true, 3, true);
        queryHistoryInfo1.setState(QueryHistoryInfo.HistoryState.SUCCESS);
        qhInfoList.add(new Pair<>(queryMetrics1.id, queryHistoryInfo1));
        QueryHistoryInfo queryHistoryInfo2 = new QueryHistoryInfo(true, 3, true);
        queryHistoryInfo2.setState(QueryHistoryInfo.HistoryState.FAILED);
        qhInfoList.add(new Pair<>(queryMetrics2.id, queryHistoryInfo2));

        queryHistoryDAO.batchUpdateQueryHistoriesInfo(qhInfoList);

        // after update
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getAllQueryHistories();

        Assert.assertEquals(queryMetrics1.id, queryHistoryList.get(2).getId());
        Assert.assertEquals(QueryHistoryInfo.HistoryState.SUCCESS,
                queryHistoryList.get(2).getQueryHistoryInfo().getState());

        Assert.assertEquals(queryMetrics2.id, queryHistoryList.get(3).getId());
        Assert.assertEquals(QueryHistoryInfo.HistoryState.FAILED,
                queryHistoryList.get(3).getQueryHistoryInfo().getState());

        Assert.assertEquals(queryMetrics3.id, queryHistoryList.get(0).getId());
        Assert.assertEquals(QueryHistoryInfo.HistoryState.PENDING,
                queryHistoryList.get(0).getQueryHistoryInfo().getState());

        Assert.assertEquals(queryMetrics4.id, queryHistoryList.get(1).getId());
        Assert.assertEquals(QueryHistoryInfo.HistoryState.PENDING,
                queryHistoryList.get(1).getQueryHistoryInfo().getState());
    }

    @Test
    public void testGetByQueryId() throws Exception {
        QueryMetrics queryMetrics = createQueryMetrics(1580311512000L, 1L, true, PROJECT, true);
        queryHistoryDAO.insert(queryMetrics);

        QueryHistory queryHistory = queryHistoryDAO.getByQueryId("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1");
        Assert.assertEquals(queryMetrics.id, queryHistory.getId());
    }

    @Test
    public void testGetRetainTime() throws Exception {
        long retainTime = RDBMSQueryHistoryDAO.getRetainTime();
        long currentTime = System.currentTimeMillis();
        Assert.assertEquals(30, (currentTime - retainTime) / (24 * 60 * 60 * 1000L));
    }

    @Test
    public void testNonAdminUserGetQueryHistories() throws Exception {
        QueryMetrics queryMetrics1 = createQueryMetrics(1580311512000L, 1L, true, PROJECT, true);
        queryMetrics1.setSubmitter(ADMIN);
        QueryMetrics queryMetrics2 = createQueryMetrics(1580397912000L, 2L, false, PROJECT, true);
        queryMetrics2.setSubmitter(ADMIN);
        QueryMetrics queryMetrics3 = createQueryMetrics(1580484312000L, 3L, false, PROJECT, true);
        queryMetrics3.setSubmitter(NORMAL_USER);
        QueryMetrics queryMetrics4 = createQueryMetrics(1895930712000L, 1L, false, "other", true);
        queryMetrics4.setSubmitter(NORMAL_USER);
        queryHistoryDAO.insert(queryMetrics1);
        queryHistoryDAO.insert(queryMetrics2);
        queryHistoryDAO.insert(queryMetrics3);
        queryHistoryDAO.insert(queryMetrics4);

        QueryHistoryRequest queryHistoryRequest = new QueryHistoryRequest();

        // system-admin and project-admin can get all query history on current project
        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(ADMIN);
        queryHistoryRequest.setProject(PROJECT);
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(3, queryHistoryList.size());

        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(NORMAL_USER);
        queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(3, queryHistoryList.size());

        // non-admin can only get self query history on current project
        queryHistoryRequest.setAdmin(false);
        queryHistoryRequest.setUsername(NORMAL_USER);
        queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(1, queryHistoryList.size());
    }

    @Test
    public void testQueryHistoryFilter() throws Exception {
        QueryMetrics queryMetrics1 = createQueryMetrics(1580311512000L, 1L, true, PROJECT, true);
        queryMetrics1.setSubmitter(ADMIN);
        queryMetrics1.setEngineType("RDBMS");
        QueryMetrics queryMetrics2 = createQueryMetrics(1580397912000L, 2L, false, PROJECT, true);
        queryMetrics2.setSubmitter(NORMAL_USER);
        queryMetrics2.setEngineType("HIVE");

        QueryMetrics queryMetrics3 = createQueryMetrics(1580484312000L, 3L, true, PROJECT, true);
        queryMetrics3.setSubmitter(NORMAL_USER);

        QueryMetrics queryMetrics4 = createQueryMetrics(1895930712000L, 1L, false, "other", true);
        queryMetrics4.setSubmitter(NORMAL_USER);
        queryHistoryDAO.insert(queryMetrics1);
        queryHistoryDAO.insert(queryMetrics2);
        queryHistoryDAO.insert(queryMetrics3);
        queryHistoryDAO.insert(queryMetrics4);

        QueryHistoryRequest queryHistoryRequest = new QueryHistoryRequest();

        // system-admin and project-admin can get all query history on current project
        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(ADMIN);
        queryHistoryRequest.setProject(PROJECT);
        queryHistoryRequest.setFilterSubmitter(Lists.newArrayList(NORMAL_USER));
        queryHistoryRequest.setRealizations(Lists.newArrayList("RDBMS", "HIVE", "ut_inner_join_cube_partial"));
        queryHistoryRequest.setFilterModelIds(Lists.newArrayList("82fa7671-a935-45f5-8779-85703601f49a.json"));
        queryHistoryRequest.setSubmitterExactlyMatch(true);
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 3, 0);
        Assert.assertEquals(2, queryHistoryList.size());

        queryHistoryRequest.setRealizations(Lists.newArrayList("pushdown", "modelName"));
        try {
            queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 3, 0);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }

        queryHistoryRequest.setRealizations(Lists.newArrayList("HIVE", "modelName"));
        queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 3, 0);
        Assert.assertEquals(2, queryHistoryList.size());

        queryHistoryRequest.setRealizations(Lists.newArrayList("HIVE", "pushdown"));
        queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 3, 0);
        Assert.assertEquals(2, queryHistoryList.size());

        queryHistoryRequest.setFilterSubmitter(Lists.newArrayList(NORMAL_USER, ADMIN));
        queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 3, 0);
        Assert.assertEquals(3, queryHistoryList.size());
    }

    @Test
    public void testGetQueryHistorySubmitters() throws Exception {
        QueryMetrics queryMetrics1 = createQueryMetrics(1580311512000L, 1L, true, PROJECT, true);
        queryMetrics1.setSubmitter(ADMIN);
        QueryMetrics queryMetrics2 = createQueryMetrics(1580397912000L, 2L, false, PROJECT, true);
        queryMetrics2.setSubmitter(ADMIN);
        QueryMetrics queryMetrics3 = createQueryMetrics(1580484312000L, 3L, false, PROJECT, true);
        queryMetrics3.setSubmitter(NORMAL_USER);
        QueryMetrics queryMetrics4 = createQueryMetrics(1895930712000L, 1L, false, "other", true);
        queryMetrics4.setSubmitter(NORMAL_USER);
        queryHistoryDAO.insert(queryMetrics1);
        queryHistoryDAO.insert(queryMetrics2);
        queryHistoryDAO.insert(queryMetrics3);
        queryHistoryDAO.insert(queryMetrics4);

        QueryHistoryRequest queryHistoryRequest = new QueryHistoryRequest();

        // system-admin and project-admin can get all query history on current project
        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(ADMIN);
        queryHistoryRequest.setProject(PROJECT);
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getQueryHistoriesSubmitters(queryHistoryRequest, 3);
        Assert.assertEquals(2, queryHistoryList.size());
    }

    @Test
    public void testGetQueryHistoryModelNames() throws Exception {
        QueryMetrics queryMetrics1 = createQueryMetrics(1580311512000L, 1L, true, PROJECT, true);
        queryMetrics1.setSubmitter(ADMIN);
        queryMetrics1.setEngineType("RDBMS");
        queryMetrics1.setQueryHistoryInfo(new QueryHistoryInfo());
        QueryMetrics queryMetrics2 = createQueryMetrics(1580397912000L, 2L, false, PROJECT, true);
        queryMetrics2.setSubmitter(ADMIN);
        queryMetrics2.setEngineType("HIVE");
        queryMetrics2.setQueryHistoryInfo(new QueryHistoryInfo());
        QueryMetrics queryMetrics3 = createQueryMetrics(1580484312000L, 3L, false, PROJECT, true);
        queryMetrics3.setSubmitter(NORMAL_USER);
        QueryMetrics queryMetrics4 = createQueryMetrics(1895930712000L, 1L, false, "other", true);
        queryMetrics4.setSubmitter(NORMAL_USER);
        queryMetrics4.setEngineType("CONSTANTS");
        queryMetrics4.setQueryHistoryInfo(new QueryHistoryInfo());
        queryHistoryDAO.insert(queryMetrics1);
        queryHistoryDAO.insert(queryMetrics2);
        queryHistoryDAO.insert(queryMetrics3);
        queryHistoryDAO.insert(queryMetrics4);

        QueryHistoryRequest queryHistoryRequest = new QueryHistoryRequest();

        // system-admin and project-admin can get all query history on current project
        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(ADMIN);
        queryHistoryRequest.setProject(PROJECT);
        List<QueryStatistics> modelList = queryHistoryDAO.getQueryHistoriesModelIds(queryHistoryRequest, 5);
        Assert.assertEquals(3, modelList.size());
        Assert.assertEquals("RDBMS", modelList.get(0).getEngineType());
        Assert.assertEquals("HIVE", modelList.get(1).getEngineType());
        Assert.assertEquals("82fa7671-a935-45f5-8779-85703601f49a.json", modelList.get(2).getModel());
    }

    @Test
    public void testReadWriteJsonForQueryHistory() throws Exception {
        // write
        QueryMetrics queryMetrics1 = createQueryMetrics(1580311512000L, 1L, true, PROJECT, true);
        queryMetrics1.setQueryHistoryInfo(new QueryHistoryInfo(true, 3, true));
        QueryMetrics queryMetrics2 = createQueryMetrics(1580397912000L, 2L, false, PROJECT, true);
        queryMetrics2.setQueryHistoryInfo(new QueryHistoryInfo(false, 5, false));
        queryHistoryDAO.insert(queryMetrics1);
        queryHistoryDAO.insert(queryMetrics2);

        // read
        QueryHistoryRequest queryHistoryRequest = new QueryHistoryRequest();
        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(ADMIN);
        queryHistoryRequest.setProject(PROJECT);
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);

        Assert.assertEquals(2, queryHistoryList.size());

        Assert.assertFalse(queryHistoryList.get(0).getQueryHistoryInfo().isExactlyMatch());
        Assert.assertEquals(5, queryHistoryList.get(0).getQueryHistoryInfo().getScanSegmentNum());
        Assert.assertEquals("PENDING", queryHistoryList.get(0).getQueryHistoryInfo().getState().toString());
        Assert.assertFalse(queryHistoryList.get(0).getQueryHistoryInfo().isExecutionError());

        Assert.assertTrue(queryHistoryList.get(1).getQueryHistoryInfo().isExactlyMatch());
        Assert.assertEquals(3, queryHistoryList.get(1).getQueryHistoryInfo().getScanSegmentNum());
        Assert.assertEquals("PENDING", queryHistoryList.get(1).getQueryHistoryInfo().getState().toString());
        Assert.assertTrue(queryHistoryList.get(1).getQueryHistoryInfo().isExecutionError());
    }

    @Test
    public void testGetQueryCountAndAvgDuration() throws Exception {
        // 2020-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1000L, true, PROJECT, true));
        // 2020-01-30 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 2000L, false, PROJECT, true));
        // 2020-01-31 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580484312000L, 3000L, false, PROJECT, true));
        // 2030-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1895930712000L, 4000L, false, PROJECT, true));

        // happy pass
        QueryStatistics statistics = queryHistoryDAO.getQueryCountAndAvgDuration(1580311512000L, 1580484312000L,
                PROJECT);
        Assert.assertEquals(2, statistics.getCount());
        Assert.assertEquals(1500, statistics.getMeanDuration(), 0.1);

        // no query history for this time period
        statistics = queryHistoryDAO.getQueryCountAndAvgDuration(1560311512000L, 1570311512000L, PROJECT);
        Assert.assertEquals(0, statistics.getCount());
        Assert.assertEquals(0, statistics.getMeanDuration(), 0.1);
    }

    @Test
    public void testGetQueryDailyStatistic() {
        // 2022-05-13 10:00:00
        queryHistoryDAO.insert(createQueryMetrics(1652407200000L, 1000L, true, PROJECT, true));
        // 2022-05-13 16:00:00
        queryHistoryDAO.insert(createQueryMetrics(1652428800000L, 2000L, true, PROJECT, true));
        // 2022-05-12 16:00:00
        queryHistoryDAO.insert(createQueryMetrics(1652342400000L, 5000L, true, PROJECT, true));
        List<QueryDailyStatistic> queryDailyStatistic = queryHistoryDAO.getQueryDailyStatistic(Long.MIN_VALUE,
                Long.MAX_VALUE);
        Assert.assertEquals(2, queryDailyStatistic.size());
        Assert.assertEquals(3000L, queryDailyStatistic.get(0).getTotalDuration());
        Assert.assertEquals(2L, queryDailyStatistic.get(0).getTotalNum());
        Assert.assertEquals(1L, queryDailyStatistic.get(1).getTotalNum());
        Assert.assertEquals(2L, queryDailyStatistic.get(0).getLt3sNum());
    }

    @Test
    public void testLargeSplitToSmallTask() {
        AtomicInteger executions = new AtomicInteger(0);
        AtomicInteger actualSize = new AtomicInteger(0);
        largeSplitToSmallTask(105, 10, currentCount -> {
            executions.incrementAndGet();
            actualSize.addAndGet(currentCount);
            if (currentCount < 10) {
                return currentCount - 1;
            } else {
                return currentCount;
            }
        }, "Test LargeSplitToSmall Task");
        Assert.assertEquals(105, actualSize.get());
        Assert.assertEquals(11, executions.get());
    }

    public static QueryMetrics createQueryMetrics(long queryTime, long duration, boolean indexHit, String project,
            boolean hitModel) {
        QueryMetrics queryMetrics = new QueryMetrics("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", "192.168.1.6:7070");
        queryMetrics.setSql("select LSTG_FORMAT_NAME from KYLIN_SALES\nLIMIT 500");
        queryMetrics.setSqlPattern("SELECT \"LSTG_FORMAT_NAME\"\nFROM \"KYLIN_SALES\"\nLIMIT 1");
        queryMetrics.setQueryDuration(duration);
        queryMetrics.setTotalScanBytes(863L);
        queryMetrics.setTotalScanCount(4096L);
        queryMetrics.setResultRowCount(500L);
        queryMetrics.setSubmitter("ADMIN");
        queryMetrics.setErrorType("");
        queryMetrics.setCacheHit(true);
        queryMetrics.setIndexHit(indexHit);
        queryMetrics.setQueryTime(queryTime);
        queryMetrics.setQueryFirstDayOfMonth(TimeUtil.getMonthStart(queryTime));
        queryMetrics.setQueryFirstDayOfWeek(TimeUtil.getWeekStart(queryTime));
        queryMetrics.setQueryDay(TimeUtil.getDayStart(queryTime));
        queryMetrics.setProjectName(project);
        queryMetrics.setQueryStatus("SUCCEEDED");
        QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo(true, 5, true);

        if (hitModel) {
            QueryMetrics.RealizationMetrics realizationMetrics = new QueryMetrics.RealizationMetrics("20000000001",
                    "Table Index", "771157c2-e6e2-4072-80c4-8ec25e1a83ea",
                    Lists.newArrayList("[DEFAULT.TEST_ACCOUNT]"));
            realizationMetrics.setQueryId("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1");
            realizationMetrics.setDuration(4591L);
            realizationMetrics.setQueryTime(1586405449387L);
            realizationMetrics.setProjectName(project);
            realizationMetrics.setModelId("82fa7671-a935-45f5-8779-85703601f49a.json");

            realizationMetrics.setSnapshots(
                    Lists.newArrayList("DEFAULT.TEST_KYLIN_ACCOUNT", "DEFAULT.TEST_COUNTRY"));

            List<QueryMetrics.RealizationMetrics> realizationMetricsList = Lists.newArrayList();
            realizationMetricsList.add(realizationMetrics);
            realizationMetricsList.add(realizationMetrics);
            queryHistoryInfo.setRealizationMetrics(realizationMetricsList);
        } else {
            queryMetrics.setEngineType(QueryHistory.EngineType.CONSTANTS.toString());
        }
        queryMetrics.setQueryHistoryInfo(queryHistoryInfo);
        return queryMetrics;
    }
}

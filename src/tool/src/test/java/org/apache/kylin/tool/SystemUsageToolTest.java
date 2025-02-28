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

package org.apache.kylin.tool;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class SystemUsageToolTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    private RDBMSQueryHistoryDAO queryHistoryDAO;

    @Before
    public void setup() throws Exception {
        JobContextUtil.cleanUp();
        createTestMetadata();
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();

        JobContextUtil.getJobInfoDao(getTestConfig());
    }

    @After
    public void teardown() {
        queryHistoryDAO.deleteAllQueryHistory();
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Test
    public void testExtractUseInfo() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        // 2022-05-13 10:00:00
        queryHistoryDAO.insert(createQueryMetrics(1652407200000L, 1000L, true, "default", true));
        SystemUsageTool.extractUseInfo(mainDir, Long.MIN_VALUE, Long.MAX_VALUE);

        File useInfoDir = new File(mainDir, "system_usage");
        Assert.assertTrue(new File(useInfoDir, "query_daily.csv").exists());
        Assert.assertTrue(new File(useInfoDir, "build_daily.csv").exists());
        Assert.assertTrue(new File(useInfoDir, "base").exists());
        List<String> lines = FileUtils.readLines(new File(useInfoDir, "query_daily.csv"), "utf-8");
        Assert.assertEquals(2, lines.size());
        Assert.assertEquals("0.0", SystemUsageTool.divide(2.0, 0.0, "%.1f"));
        Assert.assertEquals("3.3", SystemUsageTool.divide(10.0, 3.0, "%.1f"));
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

            realizationMetrics.setSnapshots(Lists.newArrayList("DEFAULT.TEST_KYLIN_ACCOUNT", "DEFAULT.TEST_COUNTRY"));

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

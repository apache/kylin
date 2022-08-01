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

package org.apache.kylin.metadata.streaming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.junit.TimeZoneTestRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import lombok.val;
import lombok.var;

@RunWith(TimeZoneTestRunner.class)

public class StreamingJobsStatsManagerTest extends NLocalFileMetadataTestCase {

    public static final String JOB_ID = "f6ca1ce7-43fc-4c42-a057-1e95dfb75d92_build";
    public static final String PROJECT_NAME = "streaming_test";

    private StreamingJobStatsManager streamingJobsStatsManager;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        streamingJobsStatsManager = StreamingJobStatsManager.getInstance();
    }

    @After
    public void destroy() throws Exception {
        streamingJobsStatsManager.deleteAllStreamingJobStats();
        cleanupTestMetadata();
        streamingJobsStatsManager.dropTable();
    }

    @Test
    public void testInsert() {
        // Test insert one at a time
        streamingJobsStatsManager
                .insert(new StreamingJobStats(JOB_ID, PROJECT_NAME, 120L, 32.22, 1200L, 100L, 1200L, 2000L));
        streamingJobsStatsManager
                .insert(new StreamingJobStats(JOB_ID, PROJECT_NAME, 120L, 8.17, 3200L, 100L, 3200L, 4500L));
        streamingJobsStatsManager
                .insert(new StreamingJobStats(JOB_ID, PROJECT_NAME, 130L, 10.22, 4200L, 100L, 4200L, 6500L));
        streamingJobsStatsManager
                .insert(new StreamingJobStats(JOB_ID, PROJECT_NAME, 100L, 25.56, 4700L, 100L, 4700L, 7200L));
        streamingJobsStatsManager
                .insert(new StreamingJobStats(JOB_ID, PROJECT_NAME, 110L, 52.11, 3100L, 100L, 3100L, 8400L));

        ConsumptionRateStats statistics1 = streamingJobsStatsManager.countAvgConsumptionRate(5000L, JOB_ID);
        Assert.assertEquals(10.22, statistics1.getMinRate(), 0.001);
        Assert.assertEquals(52.11, statistics1.getMaxRate(), 0.001);
        Assert.assertEquals(340, statistics1.getCount());

        ConsumptionRateStats statistics2 = streamingJobsStatsManager.countAvgConsumptionRate(4000L, JOB_ID);
        Assert.assertEquals(8.17, statistics2.getMinRate(), 0.001);
        Assert.assertEquals(52.11, statistics2.getMaxRate(), 0.001);
        Assert.assertEquals(460, statistics2.getCount());

        ConsumptionRateStats statistics3 = streamingJobsStatsManager.countAvgConsumptionRate(9000L, JOB_ID);
        Assert.assertNull(statistics3);
    }

    @Test
    public void testInsertList() {
        // Test insert a list
        List<StreamingJobStats> statsList = new ArrayList<>();
        statsList.add(new StreamingJobStats(JOB_ID, PROJECT_NAME, 120L, 32.22, 1200L, 100L, 1200L, 2000L));
        statsList.add(new StreamingJobStats(JOB_ID, PROJECT_NAME, 120L, 8.17, 3200L, 100L, 3200L, 4500L));
        statsList.add(new StreamingJobStats(JOB_ID, PROJECT_NAME, 130L, 10.22, 4200L, 100L, 4200L, 6500L));
        statsList.add(new StreamingJobStats(JOB_ID, PROJECT_NAME, 100L, 25.56, 4700L, 100L, 4700L, 7200L));
        statsList.add(new StreamingJobStats(JOB_ID, PROJECT_NAME, 110L, 52.11, 3100L, 100L, 3100L, 8400L));
        streamingJobsStatsManager.insert(statsList);

        ConsumptionRateStats statistics1 = streamingJobsStatsManager.countAvgConsumptionRate(5000L, JOB_ID);
        Assert.assertEquals(10.22, statistics1.getMinRate(), 0.001);
        Assert.assertEquals(52.11, statistics1.getMaxRate(), 0.001);
        Assert.assertEquals(340, statistics1.getCount());

        ConsumptionRateStats statistics2 = streamingJobsStatsManager.countAvgConsumptionRate(4000L, JOB_ID);
        Assert.assertEquals(8.17, statistics2.getMinRate(), 0.001);
        Assert.assertEquals(52.11, statistics2.getMaxRate(), 0.001);
        Assert.assertEquals(460, statistics2.getCount());
    }

    @Test
    public void testGetSJSMetricMeasurement() {
        String metricMeasurement = streamingJobsStatsManager.getSJSMetricMeasurement();
        Assert.assertEquals("test_streaming_job_stats", metricMeasurement);
    }

    @Test
    public void testGetQueryRowDetailByTime() {
        streamingJobsStatsManager.insert(
                new StreamingJobStats(JOB_ID, PROJECT_NAME, 120L, 32.22, 1200L, 100L, 1200L, getCurrentTime() + 2000L));
        streamingJobsStatsManager.insert(
                new StreamingJobStats(JOB_ID, PROJECT_NAME, 120L, 8.17, 3200L, 100L, 3200L, getCurrentTime() + 4500L));
        streamingJobsStatsManager.insert(
                new StreamingJobStats(JOB_ID, PROJECT_NAME, 130L, 10.22, 4200L, 100L, 4200L, getCurrentTime() + 6500L));
        streamingJobsStatsManager.insert(
                new StreamingJobStats(JOB_ID, PROJECT_NAME, 100L, 25.56, 4700L, 100L, 4700L, getCurrentTime() + 7200L));
        streamingJobsStatsManager.insert(
                new StreamingJobStats(JOB_ID, PROJECT_NAME, 110L, 52.11, 3100L, 100L, 3100L, getCurrentTime() + 8400L));
        List<RowCountDetailByTime> detail = streamingJobsStatsManager
                .queryRowCountDetailByTime(getCurrentTime() + 5000L, JOB_ID);

        Assert.assertEquals(3, detail.size());
        Assert.assertNotNull(detail.get(1).getCreateTime());
        Assert.assertEquals(100L, (long) detail.get(1).getBatchRowNum());
    }

    @Test
    public void testQueryStreamingJobStats() {
        String jobId = "f6ca1ce7-43fc-4c42-a057-1e95dfb75d95_build";
        val first = mockData(jobId);
        first.setCreateTime(50000L);
        streamingJobsStatsManager.insert(first);

        val second = mockData(jobId);
        second.setCreateTime(40000L);
        streamingJobsStatsManager.insert(second);

        streamingJobsStatsManager.insert(mockData(jobId));
        val list = streamingJobsStatsManager.queryStreamingJobStats(50000L, jobId);
        Assert.assertEquals(2, list.size());

    }

    @Test
    public void testGetLatestOneByJobId() {
        val jobId = "f6ca1ce7-43fc-4c42-a057-1e95dfb75d93_build";
        val first = mockData(jobId);
        first.setCreateTime(System.currentTimeMillis() - 1000);
        streamingJobsStatsManager.insert(first);
        val second = mockData(jobId);
        second.setMinDataLatency(100L);
        second.setMaxDataLatency(800L);
        second.setProcessingTime(400L);
        streamingJobsStatsManager.insert(second);
        val stat = streamingJobsStatsManager.getLatestOneByJobId(jobId);
        Assert.assertNotNull(stat);
        Assert.assertEquals(100L, stat.getMinDataLatency().longValue());
        Assert.assertEquals(800L, stat.getMaxDataLatency().longValue());

        Assert.assertEquals(400L, stat.getProcessingTime().longValue());

    }

    @Test
    public void testQueryDataLatencies() {
        val jobId = "f6ca1ce7-43fc-4c42-a057-1e95dfb75d93_build";
        val first = mockData(jobId);
        first.setCreateTime(System.currentTimeMillis() - 1000);
        streamingJobsStatsManager.insert(first);
        val second = mockData(jobId);
        second.setMinDataLatency(200L);
        streamingJobsStatsManager.insert(second);
        var dataList = streamingJobsStatsManager.queryDataLatenciesByJobIds(Collections.emptyList());
        Assert.assertTrue(dataList.isEmpty());
        val map = streamingJobsStatsManager.queryDataLatenciesByJobIds(Arrays.asList(jobId));
        Assert.assertEquals(1, map.size());
        Assert.assertEquals(200L, map.get(jobId).intValue());
    }

    private long getCurrentTime() {
        return new Date(System.currentTimeMillis()).getTime();
    }

    @Test
    public void testDropTable() {
        try {
            streamingJobsStatsManager.dropTable();
            streamingJobsStatsManager.countAvgConsumptionRate(getCurrentTime(), JOB_ID);
        } catch (PersistenceException e) {
            Assert.assertEquals("Table \"TEST_STREAMING_JOB_STATS\" not found",
                    e.getCause().getMessage().substring(0, 42));
        } catch (Exception e1) {
            return;
        } finally {
            try {
                setup();
            } catch (Exception e) {
                return;
            }
        }
    }

    @Test
    public void testDeleteAllStreamingJobStats() {
        val jobId = "f6ca1ce7-43fc-4c42-a057-1e95dfb75d93_build";
        streamingJobsStatsManager.insert(mockData(jobId));
        Assert.assertNotNull(streamingJobsStatsManager.getLatestOneByJobId(jobId));
        streamingJobsStatsManager.deleteAllStreamingJobStats();
        Assert.assertNull(streamingJobsStatsManager.getLatestOneByJobId(jobId));
    }

    @Test
    public void testDeleteSJSIfRetainTimeReached() {
        val config = getTestConfig();
        config.setProperty("kylin.streaming.jobstats.survival-time-threshold", "2d");
        val jobId = "f6ca1ce7-43fc-4c42-a057-1e95dfb75d93_build";
        streamingJobsStatsManager.insert(mockData(jobId));
        Assert.assertNotNull(streamingJobsStatsManager.getLatestOneByJobId(jobId));
        streamingJobsStatsManager.deleteSJSIfRetainTimeReached();
        Assert.assertNotNull(streamingJobsStatsManager.getLatestOneByJobId(jobId));
        streamingJobsStatsManager.deleteAllStreamingJobStats();
        Assert.assertNull(streamingJobsStatsManager.getLatestOneByJobId(jobId));

        val oldRec = mockData(jobId);
        oldRec.setCreateTime(System.currentTimeMillis() - 3 * 24 * 60 * 60 * 1000);
        streamingJobsStatsManager.insert(oldRec);
        Assert.assertNotNull(streamingJobsStatsManager.getLatestOneByJobId(jobId));

        streamingJobsStatsManager.deleteSJSIfRetainTimeReached();
        Assert.assertNull(streamingJobsStatsManager.getLatestOneByJobId(jobId));
    }

    @Test
    public void testGetRetainTime() {
        val config = getTestConfig();
        val lng = streamingJobsStatsManager.getRetainTime();
        System.out.println(System.currentTimeMillis() - lng);
        Assert.assertEquals(7, (System.currentTimeMillis() - lng) / (24 * 60 * 60 * 1000));
    }

    private StreamingJobStats mockData(String jobId) {
        val data = new StreamingJobStats(jobId, PROJECT_NAME, 120L, 32.22, 1200L, 100L, 1300L,
                System.currentTimeMillis());
        return data;
    }
}

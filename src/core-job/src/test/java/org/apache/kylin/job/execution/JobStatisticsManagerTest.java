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

package org.apache.kylin.job.execution;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.dao.JobStatistics;
import org.apache.kylin.job.dao.JobStatisticsBasic;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

public class JobStatisticsManagerTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";

    @Before
    public void setUp() {
        createTestMetadata();
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }

    @Test
    public void testUpdate() {
        JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(getTestConfig(), PROJECT);
        List<JobStatistics> jobStatistics = jobStatisticsManager.getAll();
        Assert.assertEquals(0, jobStatistics.size());

        long time = 0;
        String date = "2018-01-02";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault(Locale.Category.FORMAT));
        try {
            time = format.parse(date).getTime();
        } catch (ParseException e) {
            // ignore
        }
        jobStatisticsManager.updateStatistics(time, "test_model", 1000, 1024 * 1024, 1);
        jobStatistics = jobStatisticsManager.getAll();
        Assert.assertEquals(1, jobStatistics.size());
        Assert.assertEquals(time, jobStatistics.get(0).getDate());
        Assert.assertEquals(1, jobStatistics.get(0).getCount());
        Assert.assertEquals(1000, jobStatistics.get(0).getTotalDuration());
        Assert.assertEquals(1024 * 1024, jobStatistics.get(0).getTotalByteSize());
        Assert.assertEquals(1, jobStatistics.get(0).getJobStatisticsByModels().get("test_model").getCount());
    }

    private List<JobStatistics> getTestJobStats() {
        List<JobStatistics> jobStatistics = Lists.newArrayList();

        ZoneId zoneId = TimeZone.getTimeZone(getTestConfig().getTimeZone()).toZoneId();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd",
                Locale.getDefault(Locale.Category.FORMAT));

        String date = "2017-12-30";
        LocalDate localDate = LocalDate.parse(date, formatter);
        long time = localDate.atStartOfDay(zoneId).toInstant().toEpochMilli();
        JobStatistics jobStatistics1 = new JobStatistics(time, "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 2000, 1024);
        JobStatistics jobStatistics2 = new JobStatistics(time, "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", 2000, 1024);
        JobStatistics jobStatistics3 = new JobStatistics(time, "741ca86a-1f13-46da-a59f-95fb68615e3a", 2000, 1024);
        JobStatistics jobStatistics4 = new JobStatistics(time, "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 1000, 0);
        jobStatistics.add(jobStatistics1);
        jobStatistics.add(jobStatistics2);
        jobStatistics.add(jobStatistics3);
        jobStatistics.add(jobStatistics4);

        date = "2018-01-02";
        localDate = LocalDate.parse(date, formatter);
        time = localDate.atStartOfDay(zoneId).toInstant().toEpochMilli();
        jobStatistics1 = new JobStatistics(time, "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 2000, 1024);
        jobStatistics2 = new JobStatistics(time, "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", 1000, 1024);
        jobStatistics3 = new JobStatistics(time, "741ca86a-1f13-46da-a59f-95fb68615e3a", 1000, 1024);
        jobStatistics.add(jobStatistics1);
        jobStatistics.add(jobStatistics2);
        jobStatistics.add(jobStatistics3);

        date = "2018-01-03";
        localDate = LocalDate.parse(date, formatter);
        time = localDate.atStartOfDay(zoneId).toInstant().toEpochMilli();
        jobStatistics1 = new JobStatistics(time, "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 2000, 1024);
        jobStatistics2 = new JobStatistics(time, "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", 1000, 1024);
        jobStatistics.add(jobStatistics1);
        jobStatistics.add(jobStatistics2);

        date = "2018-01-09";
        localDate = LocalDate.parse(date, formatter);
        time = localDate.atStartOfDay(zoneId).toInstant().toEpochMilli();
        jobStatistics1 = new JobStatistics(time, "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 2000, 1024);
        jobStatistics2 = new JobStatistics(time, "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", 1000, 1024);
        jobStatistics3 = new JobStatistics(time, "741ca86a-1f13-46da-a59f-95fb68615e3a", 1000, 1024);
        jobStatistics.add(jobStatistics1);
        jobStatistics.add(jobStatistics2);
        jobStatistics.add(jobStatistics3);

        date = "2018-02-08";
        localDate = LocalDate.parse(date, formatter);
        time = localDate.atStartOfDay(zoneId).toInstant().toEpochMilli();
        jobStatistics1 = new JobStatistics(time, "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 2000, 1024);
        jobStatistics2 = new JobStatistics(time, "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", 1000, 1024);
        jobStatistics3 = new JobStatistics(time, "741ca86a-1f13-46da-a59f-95fb68615e3a", 1000, 1024);
        jobStatistics.add(jobStatistics1);
        jobStatistics.add(jobStatistics2);
        jobStatistics.add(jobStatistics3);

        date = "2018-02-09";
        localDate = LocalDate.parse(date, formatter);
        time = localDate.atStartOfDay(zoneId).toInstant().toEpochMilli();
        jobStatistics1 = new JobStatistics(time, "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 2000, 1024);
        jobStatistics2 = new JobStatistics(time, "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", 1000, 1024);
        jobStatistics3 = new JobStatistics(time, "741ca86a-1f13-46da-a59f-95fb68615e3a", 1000, 1024);
        jobStatistics4 = new JobStatistics(time, "cb596712-3a09-46f8-aea1-988b43fe9b6c", 1000, 0);
        jobStatistics.add(jobStatistics1);
        jobStatistics.add(jobStatistics2);
        jobStatistics.add(jobStatistics3);
        jobStatistics.add(jobStatistics4);

        return jobStatistics;
    }

    @Test
    public void testGetJobStats() {
        JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(getTestConfig(), PROJECT);
        List<JobStatistics> jobStatisticsForTest = getTestJobStats();
        for (JobStatistics jobStatistics : jobStatisticsForTest) {
            for (Map.Entry<String, JobStatisticsBasic> model : jobStatistics.getJobStatisticsByModels().entrySet()) {
                jobStatisticsManager.updateStatistics(jobStatistics.getDate(), model.getKey(),
                        jobStatistics.getTotalDuration(), jobStatistics.getTotalByteSize(), 1);
            }
        }

        List<JobStatistics> jobStatisticsSaved = jobStatisticsManager.getAll();
        Assert.assertEquals(6, jobStatisticsSaved.size());

        ZoneId zoneId = TimeZone.getTimeZone(getTestConfig().getTimeZone()).toZoneId();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd",
                Locale.getDefault(Locale.Category.FORMAT));
        String date = "2017-12-30";
        LocalDate localDate = LocalDate.parse(date, formatter);

        long startTime = localDate.atStartOfDay(zoneId).toInstant().toEpochMilli();
        date = "2018-03-01";
        localDate = LocalDate.parse(date, formatter);
        long endTime = localDate.atStartOfDay(zoneId).toInstant().toEpochMilli();
        // get overall job stats
        Pair<Integer, JobStatistics> jobStats = jobStatisticsManager.getOverallJobStats(startTime, endTime);
        Assert.assertEquals(19, (int) jobStats.getFirst());
        Assert.assertEquals(17408L, jobStats.getSecond().getTotalByteSize());
        Assert.assertEquals(27000L, jobStats.getSecond().getTotalDuration());

        // get job count by day
        Map<String, Integer> jobCountByTime = jobStatisticsManager.getJobCountByTime(startTime, endTime, "day");
        Assert.assertEquals(62, jobCountByTime.size());
        Assert.assertEquals(4, (int) jobCountByTime.get("2017-12-30"));
        Assert.assertEquals(2, (int) jobCountByTime.get("2018-01-03"));
        Assert.assertEquals(4, (int) jobCountByTime.get("2018-02-09"));
        Assert.assertEquals(0, (int) jobCountByTime.get("2018-01-01"));
        Assert.assertNull(jobCountByTime.get("2018-03-02"));

        // get job count by week
        date = "2018-02-01";
        localDate = LocalDate.parse(date, formatter);
        endTime = localDate.atStartOfDay(zoneId).toInstant().toEpochMilli();
        jobCountByTime = jobStatisticsManager.getJobCountByTime(startTime, endTime, "week");
        Assert.assertEquals(6, jobCountByTime.size());
        Assert.assertEquals(5, (int) jobCountByTime.get("2018-01-01"));
        Assert.assertEquals(4, (int) jobCountByTime.get("2017-12-30"));
        Assert.assertEquals(0, (int) jobCountByTime.get("2018-01-15"));

        // get job count by month
        date = "2018-03-01";
        localDate = LocalDate.parse(date, formatter);
        endTime = localDate.atStartOfDay(zoneId).toInstant().toEpochMilli();
        jobCountByTime = jobStatisticsManager.getJobCountByTime(startTime, endTime, "month");
        Assert.assertEquals(4, jobCountByTime.size());
        Assert.assertEquals(4, (int) jobCountByTime.get("2017-12-30"));
        Assert.assertEquals(8, (int) jobCountByTime.get("2018-01-01"));
        Assert.assertEquals(7, (int) jobCountByTime.get("2018-02-01"));
        Assert.assertEquals(0, (int) jobCountByTime.get("2018-03-01"));

        // get job count by model
        Map<String, Integer> jobCountByModel = jobStatisticsManager.getJobCountByModel(startTime, endTime);
        Assert.assertEquals(4, jobCountByModel.size());
        Assert.assertEquals(7, (int) jobCountByModel.get("nmodel_basic"));
        Assert.assertEquals(6, (int) jobCountByModel.get("all_fixed_length"));
        Assert.assertEquals(5, (int) jobCountByModel.get("nmodel_basic_inner"));
        Assert.assertEquals(1, (int) jobCountByModel.get("nmodel_full_measure_test"));

        // get job duration per mb by model
        Map<String, Double> jobDurationPerMbByModel = jobStatisticsManager.getDurationPerByteByModel(startTime,
                endTime);
        Assert.assertEquals(4, jobDurationPerMbByModel.size());
        Assert.assertEquals(2.2, jobDurationPerMbByModel.get("nmodel_basic"), 0.1);
        Assert.assertEquals(1.2, jobDurationPerMbByModel.get("all_fixed_length"), 0.1);
        Assert.assertEquals(1.2, jobDurationPerMbByModel.get("nmodel_basic_inner"), 0.1);
        Assert.assertEquals(0, jobDurationPerMbByModel.get("nmodel_full_measure_test"), 0.1);

        // get job duration per mb by day
        date = "2018-02-10";
        localDate = LocalDate.parse(date, formatter);
        endTime = localDate.atStartOfDay(zoneId).toInstant().toEpochMilli();
        Map<String, Double> jobDurationPerMbByTime = jobStatisticsManager.getDurationPerByteByTime(startTime, endTime,
                "day");
        Assert.assertEquals(43, jobDurationPerMbByTime.size());
        Assert.assertEquals(2.3, jobDurationPerMbByTime.get("2017-12-30"), 0.1);
        Assert.assertEquals(0, jobDurationPerMbByTime.get("2018-01-01"), 0.1);
        Assert.assertEquals(1.3, jobDurationPerMbByTime.get("2018-01-02"), 0.1);
        Assert.assertEquals(1.5, jobDurationPerMbByTime.get("2018-01-03"), 0.1);
        Assert.assertEquals(1.7, jobDurationPerMbByTime.get("2018-02-09"), 0.1);

        // get job duration per mb by week
        date = "2018-02-10";
        localDate = LocalDate.parse(date, formatter);
        endTime = localDate.atStartOfDay(zoneId).toInstant().toEpochMilli();
        jobDurationPerMbByTime = jobStatisticsManager.getDurationPerByteByTime(startTime, endTime, "week");
        Assert.assertEquals(7, jobDurationPerMbByTime.size());
        Assert.assertEquals(2.3, jobDurationPerMbByTime.get("2017-12-30"), 0.1);
        Assert.assertEquals(1.4, jobDurationPerMbByTime.get("2018-01-01"), 0.1);
        Assert.assertEquals(1.3, jobDurationPerMbByTime.get("2018-01-08"), 0.1);
        Assert.assertEquals(1.5, jobDurationPerMbByTime.get("2018-02-05"), 0.1);
        Assert.assertNull(jobDurationPerMbByTime.get("2018-02-11"));

        // get job duration per mb by month
        date = "2018-02-10";
        localDate = LocalDate.parse(date, formatter);
        endTime = localDate.atStartOfDay(zoneId).toInstant().toEpochMilli();
        jobDurationPerMbByTime = jobStatisticsManager.getDurationPerByteByTime(startTime, endTime, "month");
        Assert.assertEquals(3, jobDurationPerMbByTime.size());
        Assert.assertEquals(2.3, jobDurationPerMbByTime.get("2017-12-30"), 0.1);
        Assert.assertEquals(1.34, jobDurationPerMbByTime.get("2018-01-01"), 0.01);
        Assert.assertEquals(1.5, jobDurationPerMbByTime.get("2018-02-01"), 0.1);
    }
}

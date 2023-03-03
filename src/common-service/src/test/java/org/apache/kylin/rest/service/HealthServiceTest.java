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

import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.query.util.SlowQueryDetector;
import org.apache.kylin.rest.response.HealthResponse;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.google.common.collect.Maps;

public class HealthServiceTest extends ServiceTestBase {

    @Autowired
    @Qualifier("healthService")
    private HealthService healthService;

    @Before
    public void before() {
        SlowQueryDetector.clearCanceledSlowQueriesStatus();
    }

    @After
    public void after() {
        SlowQueryDetector.clearCanceledSlowQueriesStatus();
        SparderEnv.startSparkFailureTimes_$eq(0);
        SparderEnv.lastStartSparkFailureTime_$eq(0);
    }

    @Test
    public void testGetRestartSparkStatus() {
        int sparkFailureTimes = 1;
        long failueTime = System.currentTimeMillis();
        SparderEnv.startSparkFailureTimes_$eq(sparkFailureTimes);
        SparderEnv.lastStartSparkFailureTime_$eq(failueTime);

        HealthResponse.RestartSparkStatusResponse restartSparkStatus = healthService.getRestartSparkStatus();
        Assert.assertEquals(sparkFailureTimes, restartSparkStatus.getStartSparkFailureTimes());
        Assert.assertEquals(failueTime, restartSparkStatus.getLastStartSparkFailureTime());
    }

    @Test
    public void testGetCanceledBadQueriesStatus() {
        ConcurrentMap<String, SlowQueryDetector.CanceledSlowQueryStatus> canceledBadQueriesStatus = Maps
                .newConcurrentMap();

        String needReportQueryId = "need-report-query-id";
        //query.cancelTimes > 1 will be reported
        int needReportCanceledTimes = 2;
        String notNeedReportQueryId = "not-need-report-query-id";
        //query.cancelTimes <= 1 will not be reported
        int notNeedReportCanceledTimes = 1;
        long lastCancelTime = System.currentTimeMillis();
        float durationTime = 20.55F;

        canceledBadQueriesStatus.put(needReportQueryId, new SlowQueryDetector.CanceledSlowQueryStatus(needReportQueryId,
                needReportCanceledTimes, lastCancelTime, durationTime));
        canceledBadQueriesStatus.put(notNeedReportQueryId, new SlowQueryDetector.CanceledSlowQueryStatus(
                notNeedReportQueryId, notNeedReportCanceledTimes, lastCancelTime, durationTime));
        SlowQueryDetector.addCanceledSlowQueriesStatus(canceledBadQueriesStatus);

        List<HealthResponse.CanceledSlowQueryStatusResponse> slowQueriesStatus = healthService
                .getCanceledSlowQueriesStatus();
        Assert.assertEquals(1, slowQueriesStatus.size());
        Assert.assertEquals(needReportQueryId, slowQueriesStatus.get(0).getQueryId());
        Assert.assertEquals(needReportCanceledTimes, slowQueriesStatus.get(0).getCanceledTimes());
        Assert.assertEquals(lastCancelTime, slowQueriesStatus.get(0).getLastCanceledTime());
        Assert.assertTrue((durationTime - slowQueriesStatus.get(0).getQueryDurationTime())
                * (durationTime - slowQueriesStatus.get(0).getQueryDurationTime()) < 0.00001);
    }

}

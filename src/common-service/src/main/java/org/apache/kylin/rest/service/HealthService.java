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
import java.util.Map;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.query.util.SlowQueryDetector;
import org.apache.kylin.rest.response.HealthResponse;
import org.apache.spark.sql.SparderEnv;
import org.springframework.stereotype.Component;

@Component("healthService")
public class HealthService extends BasicService {

    public HealthResponse.RestartSparkStatusResponse getRestartSparkStatus() {
        return new HealthResponse.RestartSparkStatusResponse(SparderEnv.startSparkFailureTimes(),
                SparderEnv.lastStartSparkFailureTime());
    }

    public List<HealthResponse.CanceledSlowQueryStatusResponse> getCanceledSlowQueriesStatus() {
        List<HealthResponse.CanceledSlowQueryStatusResponse> canceledStatusList = Lists.newArrayList();

        Map<String, SlowQueryDetector.CanceledSlowQueryStatus> canceledBadQueriesStatus = SlowQueryDetector
                .getCanceledSlowQueriesStatus();

        for (SlowQueryDetector.CanceledSlowQueryStatus canceledBadQueryStatus : canceledBadQueriesStatus.values()) {
            if (canceledBadQueryStatus.getCanceledTimes() > 1) {
                canceledStatusList.add(new HealthResponse.CanceledSlowQueryStatusResponse(
                        canceledBadQueryStatus.getQueryId(), canceledBadQueryStatus.getCanceledTimes(),
                        canceledBadQueryStatus.getLastCanceledTime(), canceledBadQueryStatus.getQueryDurationTime()));
            }
        }
        return canceledStatusList;
    }

}

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

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.metrics.common.Metrics;
import org.apache.kylin.common.metrics.common.MetricsFactory;
import org.apache.kylin.common.metrics.metrics2.CodahaleMetrics;
import org.apache.kylin.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;

@Service("metricsService")
public class MetricsService extends BasicService {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsService.class);

    public String getMetrics(String type) throws Exception {

        if (!this.getConfig().getQueryMetrics2Enabled()) {
            throw new IllegalStateException("Please enable query metrics2");
        }

        Metrics instance = MetricsFactory.getInstance();
        String metric = null;
        if (!(instance instanceof CodahaleMetrics)) {
            throw new IllegalStateException("Please use CodahaleMetrics to collect your metrics");
        }
        try {
            metric = ((CodahaleMetrics) instance).dumpJson();
            if (StringUtils.isNotBlank(type)) {
                Map<String, Object> map = JsonUtil.readValue(metric, new TypeReference<Map<String, Object>>() {
                });
                metric = String.valueOf(map.get(type));
            }
        } catch (Exception e) {
            LOG.error("Dump metric json error, ", e);
        }
        return metric;
    }

}

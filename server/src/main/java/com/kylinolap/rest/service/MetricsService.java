/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.rest.service;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.codahale.metrics.MetricRegistry;
import com.kylinolap.job.JobManager;
import com.kylinolap.rest.metrics.JobMetrics;
import com.kylinolap.rest.metrics.QueryMetrics;

/**
 * @author xduo
 * 
 */
@Component("metricsService")
public class MetricsService implements InitializingBean {

    @Autowired
    @Qualifier("metrics")
    private MetricRegistry metricRegistry;

    public void registerJobMetrics(final JobManager jobManager) {
        JobMetrics jobMetrics = JobMetrics.getInstance();
        jobMetrics.setJobManager(jobManager);
        metricRegistry.register("JobMetrics", jobMetrics);
    }

    public void registerQueryMetrics() {
        metricRegistry.register("QueryMetrics", QueryMetrics.getInstance());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        registerQueryMetrics();
    }
}

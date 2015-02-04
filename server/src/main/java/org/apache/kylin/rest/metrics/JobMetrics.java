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

package org.apache.kylin.rest.metrics;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;

import java.util.Collections;
import java.util.Map;

/**
 * @author xduo
 * 
 */
public class JobMetrics implements MetricSet {

    static class JobMetricsHolder {
        static final JobMetrics INSTANCE = new JobMetrics();
    }

    public static JobMetrics getInstance() {
        return JobMetricsHolder.INSTANCE;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.codahale.metrics.MetricSet#getMetrics()
     */
    @Override
    public Map<String, Metric> getMetrics() {
        return Collections.emptyMap();
    }

}

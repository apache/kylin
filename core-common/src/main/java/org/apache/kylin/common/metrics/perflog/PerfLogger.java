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

package org.apache.kylin.common.metrics.perflog;

import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.metrics.common.Metrics;
import org.apache.kylin.common.metrics.common.MetricsFactory;
import org.apache.kylin.common.metrics.common.MetricsScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.ImmutableMap;

/**
 * PerfLogger.
 * <p>
 * Can be used to measure and log the time spent by a piece of code.
 */
public class PerfLogger implements IPerfLogger {

    static final private Logger LOG = LoggerFactory.getLogger(PerfLogger.class.getName());
    protected final Map<String, Long> startTimes = new HashMap<String, Long>();
    protected final Map<String, Long> endTimes = new HashMap<String, Long>();
    //Methods for metrics integration.  Each thread-local PerfLogger will open/close scope during each perf-log method.
    transient Map<String, MetricsScope> openScopes = new HashMap<String, MetricsScope>();

    public void perfLogBegin(String callerName, String method) {
        long startTime = System.currentTimeMillis();
        startTimes.put(method, new Long(startTime));
        if (LOG.isDebugEnabled()) {
            LOG.debug("<PERFLOG method=" + method + " from=" + callerName + ">");
        }
        beginMetrics(callerName + "." + method);
    }

    public long perfLogEnd(String callerName, String method) {
        return perfLogEnd(callerName, method, null);
    }

    public long perfLogEnd(String callerName, String method, String additionalInfo) {
        Long startTime = startTimes.get(method);
        long endTime = System.currentTimeMillis();
        endTimes.put(method, new Long(endTime));
        long duration = startTime == null ? -1 : endTime - startTime.longValue();

        if (LOG.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder("</PERFLOG method=").append(method);
            if (startTime != null) {
                sb.append(" start=").append(startTime);
            }
            sb.append(" end=").append(endTime);
            if (startTime != null) {
                sb.append(" duration=").append(duration);
            }
            sb.append(" from=").append(callerName);
            if (additionalInfo != null) {
                sb.append(" ").append(additionalInfo);
            }
            sb.append(">");
            LOG.debug(sb.toString());
        }
        endMetrics(callerName + "." + method);
        return duration;
    }

    public Long getStartTime(String method) {
        long startTime = 0L;

        if (startTimes.containsKey(method)) {
            startTime = startTimes.get(method);
        }
        return startTime;
    }

    public Long getEndTime(String method) {
        long endTime = 0L;

        if (endTimes.containsKey(method)) {
            endTime = endTimes.get(method);
        }
        return endTime;
    }

    public boolean startTimeHasMethod(String method) {
        return startTimes.containsKey(method);
    }

    public boolean endTimeHasMethod(String method) {
        return endTimes.containsKey(method);
    }

    public Long getDuration(String method) {
        long duration = 0;
        if (startTimes.containsKey(method) && endTimes.containsKey(method)) {
            duration = endTimes.get(method) - startTimes.get(method);
        }
        return duration;
    }

    public ImmutableMap<String, Long> getStartTimes() {
        return ImmutableMap.copyOf(startTimes);
    }

    public ImmutableMap<String, Long> getEndTimes() {
        return ImmutableMap.copyOf(endTimes);
    }

    private void beginMetrics(String method) {
        Metrics metrics = MetricsFactory.getInstance();
        if (metrics != null) {
            MetricsScope scope = metrics.createScope(method);
            openScopes.put(method, scope);
        }

    }

    private void endMetrics(String method) {
        Metrics metrics = MetricsFactory.getInstance();
        if (metrics != null) {
            MetricsScope scope = openScopes.remove(method);
            if (scope != null) {
                metrics.endScope(scope);
            }
        }
    }

    /**
     * Cleans up any dangling perfLog metric call scopes.
     */
    public void cleanupPerfLogMetrics() {
        Metrics metrics = MetricsFactory.getInstance();
        if (metrics != null) {
            for (MetricsScope openScope : openScopes.values()) {
                metrics.endScope(openScope);
            }
        }
        openScopes.clear();
    }
}

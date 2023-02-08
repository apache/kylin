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
package org.apache.kylin.rest.monitor;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.metrics.service.JobStatusMonitorMetric;
import org.apache.kylin.common.metrics.service.MonitorDao;
import org.apache.kylin.common.metrics.service.MonitorMetric;
import org.apache.kylin.common.metrics.service.QueryMonitorMetric;
import org.apache.kylin.common.util.AddressUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class MonitorReporter {
    private static final Logger logger = LoggerFactory.getLogger(MonitorReporter.class);
    private final String nodeType;
    private final String serverPort;

    private ScheduledExecutorService dataCollectorExecutor;
    private static final int MAX_SCHEDULED_TASKS = 5;

    private ScheduledExecutorService reportMonitorMetricsExecutor;

    private volatile boolean started = false;

    private static final long REPORT_MONITOR_METRICS_SECONDS = 1;

    private final Long periodInMilliseconds;

    @VisibleForTesting
    public int reportInitialDelaySeconds = 0;

    private static final int REPORT_QUEUE_CAPACITY = 5000;
    private LinkedBlockingDeque<MonitorMetric> reportQueue = new LinkedBlockingDeque<>(REPORT_QUEUE_CAPACITY);

    private MonitorReporter() {
        dataCollectorExecutor = Executors.newScheduledThreadPool(MAX_SCHEDULED_TASKS,
                new NamedThreadFactory("data_collector"));

        reportMonitorMetricsExecutor = Executors
                .newSingleThreadScheduledExecutor(new NamedThreadFactory("report_monitor_metrics"));

        KapConfig kapConfig = KapConfig.getInstanceFromEnv();
        this.periodInMilliseconds = kapConfig.getMonitorInterval();
        this.nodeType = kapConfig.getKylinConfig().getServerMode();
        this.serverPort = kapConfig.getKylinConfig().getServerPort();
    }

    public static MonitorReporter getInstance() {
        return Singletons.getInstance(MonitorReporter.class);
    }

    private static String getLocalIp() {
        return AddressUtil.getLocalHostExactAddress();
    }

    private static String getLocalHost() {
        String host = "localhost";
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.warn("Use the InetAddress get local host failed!", e);
        }
        return host;
    }

    private String getLocalPort() {
        return serverPort;
    }

    private static String getLocalPid() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        return runtimeMXBean.getName().split("@")[0];
    }

    private String getNodeType() {
        return this.nodeType;
    }

    private <T extends MonitorMetric> T createMonitorMetric(T monitorMetric) {
        monitorMetric.setIp(getLocalIp());
        monitorMetric.setHost(getLocalHost());
        monitorMetric.setPort(getLocalPort());
        monitorMetric.setPid(getLocalPid());
        monitorMetric.setNodeType(getNodeType());
        monitorMetric.setCreateTime(System.currentTimeMillis());

        return monitorMetric;
    }

    public QueryMonitorMetric createQueryMonitorMetric() {
        return createMonitorMetric(new QueryMonitorMetric());
    }

    public JobStatusMonitorMetric createJobStatusMonitorMetric() {
        return createMonitorMetric(new JobStatusMonitorMetric());
    }

    public Integer getQueueSize() {
        return reportQueue.size();
    }

    private void reportMonitorMetrics() {
        try {
            int queueSize = reportQueue.size();
            for (int i = 0; i < queueSize; i++) {
                MonitorMetric monitorMetric = reportQueue.poll(100, TimeUnit.MILLISECONDS);
                if (null == monitorMetric) {
                    logger.warn("Found the MonitorMetric poll from reportQueue is null!");
                    continue;
                }

                MonitorDao.getInstance()
                        .write2InfluxDB(MonitorDao.getInstance().convert2InfluxDBWriteRequest(monitorMetric));
            }
        } catch (Exception e) {
            logger.error("Failed to report monitor metrics to db!", e);
        }
    }

    public void startReporter() {
        reportMonitorMetricsExecutor.scheduleWithFixedDelay(this::reportMonitorMetrics, reportInitialDelaySeconds,
                REPORT_MONITOR_METRICS_SECONDS, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(this::stopReporter));
        started = true;

        logger.info("MonitorReporter started!");
    }

    @VisibleForTesting
    public void stopReporter() {
        ExecutorServiceUtil.forceShutdown(dataCollectorExecutor);
        ExecutorServiceUtil.forceShutdown(reportMonitorMetricsExecutor);
        started = false;

        logger.info("MonitorReporter stopped!");
    }

    public void submit(AbstractMonitorCollectTask collectTask) {
        // for UT
        if (!started) {
            logger.warn("MonitorReporter is not started!");
            return;
        }

        if (!collectTask.getRunningServerMode().contains(getNodeType())) {
            logger.info("This node can not run this collect task, serverMode: {}, task serverMode: {}!", getNodeType(),
                    StringUtils.join(collectTask.getRunningServerMode(), ","));
            return;
        }

        dataCollectorExecutor.scheduleWithFixedDelay(collectTask, 0, periodInMilliseconds, TimeUnit.MILLISECONDS);
    }

    public synchronized boolean reportMonitorMetric(MonitorMetric monitorMetric) {
        Preconditions.checkArgument(started, "MonitorReporter is not started!");

        try {
            this.reportQueue.add(monitorMetric);
        } catch (IllegalStateException ie) {
            logger.warn("Monitor metrics report queue is full!", ie);
            return false;
        } catch (Exception e) {
            logger.error("Failed to report MonitorMetric!", e);
            return false;
        }
        return true;
    }
}

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

package org.apache.kylin.common.metrics;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.metrics.reporter.InfluxdbReporter;
import org.apache.kylin.common.metrics.service.InfluxDBInstance;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.shaded.influxdb.org.influxdb.dto.Query;
import org.apache.kylin.shaded.influxdb.org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;

public class MetricsInfluxdbReporter implements MetricsReporter {

    private static final Logger logger = LoggerFactory.getLogger(MetricsInfluxdbReporter.class);

    public static final String METRICS_MEASUREMENT = "system_metric";

    public static final String KYLIN_METRICS_RP = "KYLIN_METRICS_RP";
    public static final String DAILY_METRICS_RETENTION_POLICY_NAME = "KYLIN_METRICS_DAILY_RP";
    public static final String DAILY_METRICS_MEASUREMENT = "system_metric_daily";
    private AtomicInteger retry = new AtomicInteger(0);
    private AtomicLong lastUpdateTime = new AtomicLong(0);

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final String reporterName = "MetricsReporter";

    private String defaultMeasurement = null;
    private InfluxdbReporter underlying = null;
    private InfluxDBInstance dailyInstance = null;
    @Getter
    private InfluxDBInstance metricInstance = null;

    public static MetricsInfluxdbReporter getInstance() {
        return Singletons.getInstance(MetricsInfluxdbReporter.class);
    }

    private MetricsInfluxdbReporter() {
        // do nothing
    }

    private void updateDailyMetrics(long todayStart, MetricsConfig config) {
        long yesterdayStart = TimeUtil.minusDays(todayStart, 1);
        this.underlying.getMetrics().forEach(point -> {
            StringBuilder sql = new StringBuilder("SELECT ");
            sql.append(StringUtils.join(point.getFields().keySet().stream()
                    .map(field -> String.format(Locale.ROOT, " LAST(\"%s\") AS \"%s\" ", field, field))
                    .collect(Collectors.toList()), ","));
            sql.append(String.format(Locale.ROOT, " FROM %s WHERE ", METRICS_MEASUREMENT));
            sql.append(String.format(Locale.ROOT, " time >= %dms AND time < %dms ", yesterdayStart, todayStart));
            point.getTags()
                    .forEach((tag, value) -> sql.append(String.format(Locale.ROOT, " AND %s='%s' ", tag, value)));

            QueryResult queryResult = this.underlying.getInfluxDb()
                    .query(new Query(sql.toString(), config.getMetricsDB()));

            if (CollectionUtils.isEmpty(queryResult.getResults())
                    || CollectionUtils.isEmpty(queryResult.getResults().get(0).getSeries())
                    || CollectionUtils.isEmpty(queryResult.getResults().get(0).getSeries().get(0).getValues())) {
                logger.warn("Failed to aggregate metric, cause query result is empty, uKey: {}!", point.getUniqueKey());
                return;
            }

            List<String> columns = queryResult.getResults().get(0).getSeries().get(0).getColumns();
            List<Object> firstLine = queryResult.getResults().get(0).getSeries().get(0).getValues().get(0);
            Map<String, Object> fields = Maps.newHashMap();
            for (int i = 1; i < columns.size(); i++) {
                fields.put(columns.get(i), firstLine.get(i));
            }

            dailyInstance.write(DAILY_METRICS_MEASUREMENT, point.getTags(), fields, yesterdayStart);
        });
    }

    private void startDailyReport(MetricsConfig config) {
        dailyInstance = new InfluxDBInstance(config.getDailyMetricsDB(), DAILY_METRICS_RETENTION_POLICY_NAME, "0d",
                "30d", 2, true);
        dailyInstance.init();
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(() -> {
            try {
                logger.debug("Start to aggregate daily metrics ...");
                long now = System.currentTimeMillis();
                long todayStart = TimeUtil.getDayStart(now);

                // init and retry not check
                // lastUpdateTime < todayStart and config.getDailyMetricsRunHour() == TimeUtil.getHour(now) will run
                if (lastUpdateTime.get() > 0
                        && (retry.get() == 0 || retry.get() > config.getDailyMetricsMaxRetryTimes())) {
                    if (lastUpdateTime.get() > todayStart) {
                        return;
                    }

                    if (config.getDailyMetricsRunHour() != TimeUtil.getHour(now)) {
                        return;
                    }
                    retry.set(0);
                }

                // no metrics or metrics not
                if (CollectionUtils.isEmpty(this.underlying.getMetrics())) {
                    return;
                }

                lastUpdateTime.set(now);
                updateDailyMetrics(todayStart, config);

                retry.set(0);
                logger.debug("Aggregate daily metrics success ...");
            } catch (Exception e) {
                retry.incrementAndGet();
                logger.error("Failed to aggregate daily metrics, retry: {}", retry.get(), e);
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    @Override
    public void init(KapConfig kapConfig) {

        synchronized (this) {
            if (!initialized.get()) {
                final MetricsConfig config = new MetricsConfig(kapConfig);
                defaultMeasurement = METRICS_MEASUREMENT;
                metricInstance = new InfluxDBInstance(config.getMetricsDB(), KYLIN_METRICS_RP, "30d", "7d", 1, true);
                metricInstance.init();
                underlying = new InfluxdbReporter(metricInstance, defaultMeasurement,
                        MetricsController.getDefaultMetricRegistry(), reporterName);
                initialized.set(true);
            }
        }
    }

    @Override
    public void start(KapConfig kapConfig) {
        synchronized (this) {
            if (initialized.get()) {
                final MetricsConfig config = new MetricsConfig(kapConfig);
                startReporter(config.pollingIntervalSecs());
                startDailyReport(config);
            }
        }
    }

    @Override
    public void startReporter(int pollingPeriodInSeconds) {
        synchronized (this) {
            if (initialized.get() && !running.get()) {
                underlying.report();
                underlying.start(pollingPeriodInSeconds, TimeUnit.SECONDS);
                running.set(true);
                logger.info("kylin.metrics influxdb reporter started");
            }
        }
    }

    @Override
    public void stopReporter() {
        synchronized (this) {
            if (initialized.get() && running.get()) {
                underlying.stop();
                underlying.close();
                running.set(false);
            }
        }
    }

    @Override
    public String getMBeanName() {
        return "kylin.metrics:type=NMetricsInfluxdbReporter";
    }

    @VisibleForTesting
    public boolean isRunning() {
        return running.get();
    }
}

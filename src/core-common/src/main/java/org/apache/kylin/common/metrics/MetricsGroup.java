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

import static java.util.stream.Collectors.toMap;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.metrics.gauges.QueryRatioGauge;
import org.apache.kylin.common.util.AddressUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Query;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.QueryResult;

public class MetricsGroup {

    private static final Logger logger = LoggerFactory.getLogger(MetricsGroup.class);

    public static final Set<String> gauges = Collections.synchronizedSet(new HashSet<>());

    public static final ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<>();

    public static final ConcurrentHashMap<String, Meter> meters = new ConcurrentHashMap<>();

    public static final ConcurrentHashMap<String, Histogram> histograms = new ConcurrentHashMap<>();

    private MetricsGroup() {
    }

    public static boolean hostTagCounterInc(MetricsName name, MetricsCategory category, String entity) {
        return counterInc(name, category, entity, getHostTagMap(entity));
    }

    public static boolean hostTagCounterInc(MetricsName name, MetricsCategory category, String entity,
            long increments) {
        return counterInc(name, category, entity, getHostTagMap(entity), increments);
    }

    public static boolean counterInc(MetricsName name, MetricsCategory category, String entity) {
        return counterInc(name, category, entity, Collections.emptyMap());
    }

    public static boolean counterInc(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags) {
        return counterInc(name, category, entity, tags, 1);
    }

    public static boolean counterInc(MetricsName name, MetricsCategory category, String entity, long increments) {
        return counterInc(name, category, entity, Collections.emptyMap(), increments);
    }

    public static boolean counterInc(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags, long increments) {
        if (increments < 0) {
            return false;
        }
        try {
            final Counter counter = registerCounterIfAbsent(name.getVal(), category.getVal(), entity, tags);
            counter.inc(increments);
            return true;
        } catch (Exception e) {
            logger.warn("ke.metrics counterInc {}", e.getMessage());
        }
        return false;
    }

    public static boolean hostTagHistogramUpdate(MetricsName name, MetricsCategory category, String entity,
            long updateTo) {
        return histogramUpdate(name, category, entity, getHostTagMap(entity), updateTo);
    }

    public static boolean histogramUpdate(MetricsName name, MetricsCategory category, String entity, long updateTo) {
        return histogramUpdate(name, category, entity, Collections.emptyMap(), updateTo);
    }

    public static boolean histogramUpdate(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags, long updateTo) {
        if (updateTo < 0) {
            return false;
        }
        try {
            final Histogram histogram = registerHistogramIfAbsent(name.getVal(), category.getVal(), entity, tags);
            histogram.update(updateTo);
            return true;
        } catch (Exception e) {
            logger.warn("ke.metrics histogramUpdate {}", e.getMessage());
        }
        return false;
    }

    public static boolean meterMark(MetricsName name, MetricsCategory category, String entity) {
        return meterMark(name, category, entity, Collections.emptyMap());
    }

    public static boolean meterMark(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags) {
        try {
            final Meter meter = registerMeterIfAbsent(name.getVal(), category.getVal(), entity, tags);
            meter.mark();
            return true;
        } catch (Exception e) {
            logger.warn("ke.metrics meterMark {}", e.getMessage());
        }
        return false;
    }

    public static boolean removeGlobalMetrics() {
        final String metricNameSuffix = metricNameSuffix(MetricsCategory.GLOBAL.getVal(), "global",
                Collections.emptyMap());
        final MetricRegistry registry = MetricsController.getDefaultMetricRegistry();
        removeMetrics(metricNameSuffix, registry);
        return true;
    }

    public static boolean removeProjectMetrics(final String projectName) {
        if (StringUtils.isEmpty(projectName)) {
            throw new IllegalArgumentException("removeProjectMetrics, projectName shouldn't be empty.");
        }
        final String metricNameSuffix = metricNameSuffix(MetricsCategory.PROJECT.getVal(), projectName,
                Collections.emptyMap());

        final MetricRegistry registry = MetricsController.getDefaultMetricRegistry();
        removeMetrics(metricNameSuffix, registry);
        return true;
    }

    public static boolean removeModelMetrics(String project, String modelId) {
        if (StringUtils.isEmpty(project)) {
            throw new IllegalArgumentException("removeModelMetrics, projectName shouldn't be empty.");
        }
        if (StringUtils.isEmpty(modelId)) {
            throw new IllegalArgumentException("removeModelMetrics, modelId shouldn't be empty.");
        }
        Map<String, String> tags = Maps.newHashMap();
        tags.put(MetricsTag.MODEL.getVal(), modelId);
        final String metricNameSuffix = metricNameSuffix(MetricsCategory.PROJECT.getVal(), project, tags);
        final MetricRegistry registry = MetricsController.getDefaultMetricRegistry();

        removeMetrics(metricNameSuffix, registry);
        return true;
    }

    private static void removeMetrics(String metricNameSuffix, MetricRegistry registry) {
        synchronized (gauges) {
            final Iterator<String> it = gauges.iterator();
            doRemove(metricNameSuffix, it, registry);
        }

        synchronized (counters) {
            final Iterator<String> it = counters.keySet().iterator();
            doRemove(metricNameSuffix, it, registry);
        }

        synchronized (meters) {
            final Iterator<String> it = meters.keySet().iterator();
            doRemove(metricNameSuffix, it, registry);
        }

        synchronized (histograms) {
            final Iterator<String> it = histograms.keySet().iterator();
            doRemove(metricNameSuffix, it, registry);
        }
    }

    public static Counter getCounter(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags) {
        final String metricName = metricName(name.getVal(), category.getVal(), entity, tags);
        return counters.get(metricName);
    }

    public static <T> Gauge<T> getGauge(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags) {
        final String metricName = metricName(name.getVal(), category.getVal(), entity, tags);
        return MetricsController.getDefaultMetricRegistry().getGauges().get(metricName);
    }

    public static boolean registerProjectMetrics(final String projectName, final String host) {
        if (StringUtils.isEmpty(projectName)) {
            throw new IllegalArgumentException("registerProjectMetrics, projectName shouldn't be empty.");
        }

        Map<String, String> tags = getHostTagMap(host, projectName);

        // transaction
        newCounter(MetricsName.TRANSACTION_RETRY_COUNTER, MetricsCategory.PROJECT, projectName, tags);
        newHistogram(MetricsName.TRANSACTION_LATENCY, MetricsCategory.PROJECT, projectName, tags);
        // query
        newCounter(MetricsName.QUERY, MetricsCategory.PROJECT, projectName, tags);
        Counter denominator = getCounter(MetricsName.QUERY, MetricsCategory.PROJECT, projectName, tags);
        newCounter(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, projectName, tags);
        Counter numerator = getCounter(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, projectName, tags);
        newGauge(MetricsName.QUERY_LT_1S_RATIO, MetricsCategory.PROJECT, projectName, tags,
                new QueryRatioGauge(numerator, denominator));
        newCounter(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, projectName, tags);
        numerator = getCounter(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, projectName, tags);
        newGauge(MetricsName.QUERY_1S_3S_RATIO, MetricsCategory.PROJECT, projectName, tags,
                new QueryRatioGauge(numerator, denominator));
        newCounter(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, projectName, tags);
        numerator = getCounter(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, projectName, tags);
        newGauge(MetricsName.QUERY_3S_5S_RATIO, MetricsCategory.PROJECT, projectName, tags,
                new QueryRatioGauge(numerator, denominator));
        newCounter(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, projectName, tags);
        numerator = getCounter(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, projectName, tags);
        newGauge(MetricsName.QUERY_5S_10S_RATIO, MetricsCategory.PROJECT, projectName, tags,
                new QueryRatioGauge(numerator, denominator));
        newCounter(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, projectName, tags);
        numerator = getCounter(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, projectName, tags);
        newGauge(MetricsName.QUERY_SLOW_RATIO, MetricsCategory.PROJECT, projectName, tags,
                new QueryRatioGauge(numerator, denominator));
        newCounter(MetricsName.QUERY_FAILED, MetricsCategory.PROJECT, projectName, tags);
        newCounter(MetricsName.QUERY_PUSH_DOWN, MetricsCategory.PROJECT, projectName, tags);
        numerator = getCounter(MetricsName.QUERY_PUSH_DOWN, MetricsCategory.PROJECT, projectName, tags);
        newGauge(MetricsName.QUERY_PUSH_DOWN_RATIO, MetricsCategory.PROJECT, projectName, tags,
                new QueryRatioGauge(numerator, denominator));
        newCounter(MetricsName.QUERY_CONSTANTS, MetricsCategory.PROJECT, projectName, tags);
        numerator = getCounter(MetricsName.QUERY_CONSTANTS, MetricsCategory.PROJECT, projectName, tags);
        newGauge(MetricsName.QUERY_CONSTANTS_RATIO, MetricsCategory.PROJECT, projectName, tags,
                new QueryRatioGauge(numerator, denominator));
        newCounter(MetricsName.QUERY_CACHE, MetricsCategory.PROJECT, projectName, tags);
        numerator = getCounter(MetricsName.QUERY_CACHE, MetricsCategory.PROJECT, projectName, tags);
        newGauge(MetricsName.QUERY_CACHE_RATIO, MetricsCategory.PROJECT, projectName, tags,
                new QueryRatioGauge(numerator, denominator));
        newCounter(MetricsName.QUERY_AGG_INDEX, MetricsCategory.PROJECT, projectName, tags);
        numerator = getCounter(MetricsName.QUERY_AGG_INDEX, MetricsCategory.PROJECT, projectName, tags);
        newGauge(MetricsName.QUERY_AGG_INDEX_RATIO, MetricsCategory.PROJECT, projectName, tags,
                new QueryRatioGauge(numerator, denominator));
        newCounter(MetricsName.QUERY_TABLE_INDEX, MetricsCategory.PROJECT, projectName, tags);
        numerator = getCounter(MetricsName.QUERY_TABLE_INDEX, MetricsCategory.PROJECT, projectName, tags);
        newGauge(MetricsName.QUERY_TABLE_INDEX_RATIO, MetricsCategory.PROJECT, projectName, tags,
                new QueryRatioGauge(numerator, denominator));
        newCounter(MetricsName.QUERY_TOTAL_DURATION, MetricsCategory.PROJECT, projectName, tags);
        newCounter(MetricsName.QUERY_TIMEOUT, MetricsCategory.PROJECT, projectName, tags);
        newMeter(MetricsName.QUERY_SLOW_RATE, MetricsCategory.PROJECT, projectName, tags);
        newMeter(MetricsName.QUERY_FAILED_RATE, MetricsCategory.PROJECT, projectName, tags);
        newMeter(MetricsName.QUERY_PUSH_DOWN_RATE, MetricsCategory.PROJECT, projectName, tags);
        newMeter(MetricsName.QUERY_CONSTANTS_RATE, MetricsCategory.PROJECT, projectName, tags);
        newMeter(MetricsName.QUERY_TIMEOUT_RATE, MetricsCategory.PROJECT, projectName, tags);
        newHistogram(MetricsName.QUERY_LATENCY, MetricsCategory.PROJECT, projectName, tags);
        // job
        newCounter(MetricsName.JOB, MetricsCategory.PROJECT, projectName, tags);
        newCounter(MetricsName.JOB_DURATION, MetricsCategory.PROJECT, projectName, tags);
        newCounter(MetricsName.JOB_FINISHED, MetricsCategory.PROJECT, projectName, tags);
        newCounter(MetricsName.JOB_STEP_ATTEMPTED, MetricsCategory.PROJECT, projectName, tags);
        newCounter(MetricsName.JOB_FAILED_STEP_ATTEMPTED, MetricsCategory.PROJECT, projectName, tags);
        newCounter(MetricsName.JOB_RESUMED, MetricsCategory.PROJECT, projectName, tags);
        newCounter(MetricsName.JOB_DISCARDED, MetricsCategory.PROJECT, projectName, tags);
        newCounter(MetricsName.JOB_ERROR, MetricsCategory.PROJECT, projectName, tags);
        newHistogram(MetricsName.JOB_DURATION_HISTOGRAM, MetricsCategory.PROJECT, projectName, tags);
        newCounter(MetricsName.JOB_WAIT_DURATION, MetricsCategory.PROJECT, projectName, tags);
        // metadata management
        newCounter(MetricsName.METADATA_CLEAN, MetricsCategory.PROJECT, projectName, tags);
        newCounter(MetricsName.METADATA_BACKUP, MetricsCategory.PROJECT, projectName, tags);
        newCounter(MetricsName.METADATA_BACKUP_DURATION, MetricsCategory.PROJECT, projectName, tags);
        newCounter(MetricsName.METADATA_BACKUP_FAILED, MetricsCategory.PROJECT, projectName, tags);

        newHistogram(MetricsName.QUERY_SCAN_BYTES, MetricsCategory.PROJECT, projectName, tags);

        return true;
    }

    public static void newMetricSet(MetricsName name, MetricsCategory category, String entity, MetricSet metricSet) {
        newMetrics(name.getVal(), metricSet, category, entity);
    }

    private static void newMetrics(String name, MetricSet metricSet, MetricsCategory category, String entity) {
        for (Map.Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
            Metric value = entry.getValue();
            if (value instanceof MetricSet) {
                newMetrics(name(name, entry.getKey()), (MetricSet) value, category, entity);
            } else {
                newGauge(name(name, entry.getKey()), category, entity, Collections.emptyMap(), value);
            }
        }
    }

    private static String name(String prefix, String part) {
        return "".concat(prefix).concat(".").concat(part);
    }

    public static <T> boolean newGauge(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags, Gauge<T> metric) {
        return newGauge(name.getVal(), category, entity, tags, metric);
    }

    public static <T> boolean newGauge(MetricsName name, MetricsCategory category, String entity, Gauge<T> metric) {
        return newGauge(name.getVal(), category, entity, Collections.emptyMap(), metric);
    }

    private static boolean newGauge(String name, MetricsCategory category, String entity, Map<String, String> tags,
            Metric metric) {
        return registerGaugeIfAbsent(name, category, entity, tags, metric);
    }

    public static boolean newCounter(MetricsName name, MetricsCategory category, String entity) {
        return newCounter(name, category, entity, Collections.emptyMap());
    }

    public static boolean newCounter(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags) {
        registerCounterIfAbsent(name.getVal(), category.getVal(), entity, tags);
        return true;
    }

    public static boolean newHistogram(MetricsName name, MetricsCategory category, String entity) {
        return newHistogram(name, category, entity, Collections.emptyMap());
    }

    public static boolean newHistogram(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags) {
        try {
            registerHistogramIfAbsent(name.getVal(), category.getVal(), entity, tags);
            return true;
        } catch (Exception e) {
            logger.warn("ke.metrics newHistogram {}", e.getMessage());
        }
        return false;
    }

    public static boolean newMeter(MetricsName name, MetricsCategory category, String entity) {
        return newMeter(name, category, entity, Collections.emptyMap());
    }

    private static boolean newMeter(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags) {
        registerMeterIfAbsent(name.getVal(), category.getVal(), entity, tags);
        return true;
    }

    private static SortedMap<String, String> filterTags(Map<String, String> tags) {
        return new TreeMap<>(tags.entrySet().stream().filter(e -> !"category".equals(e.getKey()))
                .filter(e -> !"entity".equals(e.getKey())).filter(e -> StringUtils.isNotEmpty(e.getValue()))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    public static String metricName(String name, String category, String entity, Map<String, String> tags) {
        Preconditions.checkNotNull(name);
        StringBuilder sb = new StringBuilder(name);
        sb.append(":").append(metricNameSuffix(category, entity, tags));
        return sb.toString();
    }

    private static String metricNameSuffix(String category, String entity, Map<String, String> tags) {
        StringBuilder sb = new StringBuilder();
        sb.append("category=");
        sb.append(category);
        sb.append(",entity=");
        sb.append(entity);

        if (!MapUtils.isEmpty(tags)) {
            final SortedMap<String, String> filteredTags = filterTags(tags);
            if (!filteredTags.isEmpty()) {
                sb.append(",").append(filteredTags.entrySet().stream()
                        .map(e -> String.join("=", e.getKey(), e.getValue())).collect(Collectors.joining(",")));
            }
        }
        return sb.toString();
    }

    private static boolean registerGaugeIfAbsent(String name, MetricsCategory category, String entity,
            Map<String, String> tags, Metric metric) {
        final String metricName = metricName(name, category.getVal(), entity, tags);
        if (!gauges.contains(metricName)) {
            synchronized (gauges) {
                if (!gauges.contains(metricName)) {
                    MetricsController.getDefaultMetricRegistry().register(metricName, metric);
                    gauges.add(metricName);
                    logger.trace("ke.metrics register gauge: {}", metricName);
                    return true;
                }
            }
        }

        return false;
    }

    private static Counter registerCounterIfAbsent(String name, String category, String entity,
            Map<String, String> tags) {
        final String metricName = metricName(name, category, entity, tags);
        if (!counters.containsKey(metricName)) {
            synchronized (counters) {
                if (!counters.containsKey(metricName)) {
                    // bad design: 1. Consider async realization; 2. Deadlock maybe occurs here; 3. Add timeout mechanism.
                    final Counter metric = MetricsController.getDefaultMetricRegistry().counter(metricName);
                    final long restoreVal = tryRestoreCounter(name, category, entity, tags);
                    if (restoreVal > 0) {
                        metric.inc(restoreVal);
                        logger.trace("ke.metrics counter=[{}] restore with value: {}", metricName, restoreVal);
                    }
                    counters.put(metricName, metric);
                    logger.trace("ke.metrics register counter: {}", metricName);
                }
            }
        }
        return counters.get(metricName);
    }

    public static final Map<String, MetricsObject> cacheInfluxMetricStatusMap = new ConcurrentHashMap<>();

    public static long tryRestoreCounter(String fieldName, String category, String entity, Map<String, String> tags) {
        try {
            MetricsObject metricsObject = new MetricsObject(fieldName, category, entity, tags);
            if (MetricsName.QUERY.getVal().equals(fieldName)) {
                String cacheKey = metricsObject.toString();
                cacheInfluxMetricStatusMap.put(cacheKey, metricsObject);
                long restoreVal = tryRestoreExceptionCounter(fieldName, category, entity, tags);
                cacheInfluxMetricStatusMap.get(cacheKey).setInitStatus(true);
                return restoreVal;
            }
            if (KylinConfig.getInstanceFromEnv().isDevOrUT()) {
                return 0;
            }
            final InfluxDB defaultInfluxDb = MetricsInfluxdbReporter.getInstance().getMetricInstance().getInfluxDB();
            if (!defaultInfluxDb.ping().isGood()) {
                throw new IllegalStateException("the pinged influxdb is not good.");
            }

            final KapConfig config = KapConfig.getInstanceFromEnv();

            final String querySql = getValFromInfluxQuerySql("", metricsObject, null);

            final QueryResult result = defaultInfluxDb
                    .query(new Query(querySql, config.getMetricsDbNameWithMetadataUrlPrefix()));
            return getResultFromSeries(result, fieldName);
        } catch (Exception e) {
            logger.warn(
                    "ke.metrics tryRestoreCounter error. fieldName: [{}], category [{}], entity [{}], tags [{}]. error msg {}",
                    fieldName, category, entity, tags, e.getMessage());
        }
        return 0;
    }

    public static void monitorRegisterMetrics() {
        cacheInfluxMetricStatusMap.keySet().forEach(cacheKey -> {
            MetricsObject metricsObject = cacheInfluxMetricStatusMap.get(cacheKey);
            if (!metricsObject.isInitStatus()) {
                String name = metricsObject.getFieldName();
                String category = metricsObject.getCategory();
                String entity = metricsObject.getEntity();
                Map<String, String> tags = metricsObject.getTags();
                final String metricName = metricName(name, category, entity, tags);
                synchronized (counters) {
                    final Counter metric = MetricsController.getDefaultMetricRegistry().counter(metricName);
                    final long restoreVal = tryRestoreExceptionCounter(name, category, entity, tags);
                    if (restoreVal > 0) {
                        metric.dec(metric.getCount());
                        metric.inc(restoreVal);
                        logger.info("Restore monitorRegisterMetrics...metric.getCount()={}"
                                        + ", name={}, entity={}, tags={}",
                                metric.getCount(), name, entity, tags);
                    }
                    counters.put(metricName, metric);
                }
            }
        });
    }

    private static long tryRestoreExceptionCounter(String fieldName, String category, String entity, Map<String, String> tags) {
        MetricsObject metricsObject = new MetricsObject(fieldName, category, entity, tags);
        if (KylinConfig.getInstanceFromEnv().isDevOrUT()) {
            cacheInfluxMetricStatusMap.get(metricsObject.toString()).setInitStatus(true);
            return 9999999999L;
        }
        return tryRestoreExceptionCounter(metricsObject);
    }

    public static long tryRestoreExceptionCounter(MetricsObject metricsObject) {
        try {
            final InfluxDB defaultInfluxDb = MetricsInfluxdbReporter.getInstance().getMetricInstance().getInfluxDB();
            if (!defaultInfluxDb.ping().isGood()) {
                throw new IllegalStateException("the pinged influxdb is not good.");
            }
            return tryRestoreExceptionCounter(metricsObject, defaultInfluxDb);
        } catch (Exception e) {
            logger.warn(
                    "Influx tryRestoreExceptionCounter error. metricsObject={}",
                    metricsObject, e);
        }
        return 0;
    }

    public static long tryRestoreExceptionCounter(MetricsObject metricsObject, InfluxDB defaultInfluxDb) {
        final KapConfig config = KapConfig.getInstanceFromEnv();
        long currTime = System.currentTimeMillis();
        long startTime = currTime - 7 * 24 * 3600 * 1000;
        String startTimeFilter = " and time > " + startTime + " ";
        long queryTotalTimesFirst = getValFromInflux(MetricsObjectType.FIRST.getVal(),
                defaultInfluxDb, config, metricsObject, startTimeFilter);
        long queryTotalTimesFirstMaxGThanFirst = getValFromInflux(MetricsObjectType.MAX.getVal(),
                defaultInfluxDb, config, metricsObject, startTimeFilter + " and query_total_times >= " + queryTotalTimesFirst + " ");
        long queryTotalTimesFirstMaxLThanFirst = getValFromInflux(MetricsObjectType.MAX.getVal(),
                defaultInfluxDb, config, metricsObject, startTimeFilter + " and query_total_times < " + queryTotalTimesFirst + " ");
        cacheInfluxMetricStatusMap.get(metricsObject.toString()).setInitStatus(true);
        return queryTotalTimesFirstMaxGThanFirst + queryTotalTimesFirstMaxLThanFirst;
    }

    public static long getValFromInflux(String metricsObjectType, InfluxDB defaultInfluxDb,
           KapConfig kapConfig, MetricsObject metricsObject, String filter) {
        try {
            String fieldName = metricsObject.getFieldName();
            final String querySql = getValFromInfluxQuerySql(metricsObjectType, metricsObject, filter);

            final QueryResult result = defaultInfluxDb
                    .query(new Query(querySql, kapConfig.getMetricsDbNameWithMetadataUrlPrefix()));
            return getResultFromSeries(result, fieldName);
        } catch (Exception e) {
            logger.error("GetValFromInflux error metricsObjectType={}, metricsObject={}",
                    metricsObjectType, metricsObject, e);
            return -1;
        }
    }

    public static long getResultFromSeries(QueryResult result, String fieldName) throws ParseException {
        if (CollectionUtils.isEmpty(result.getResults().get(0).getSeries())) {
            return 0;
        }
        QueryResult.Series series = result.getResults().get(0).getSeries().get(0);
        String valStr = fieldName.equals(series.getColumns().get(1))
                ? String.valueOf(series.getValues().get(0).get(1))
                : String.valueOf(series.getValues().get(0).get(0));
        return NumberFormat.getInstance(Locale.getDefault(Locale.Category.FORMAT)).parse(valStr).longValue();
    }

    public static String getValFromInfluxQuerySql(String metricsObjectType, MetricsObject metricsObject, String filter) {
        String fieldName = metricsObject.getFieldName();
        String category = metricsObject.getCategory();
        String entity = metricsObject.getEntity();
        Map<String, String> tags = metricsObject.getTags();
        final StringBuilder sb = new StringBuilder("select ");
        String tempSql = "(" + fieldName + ") as " + fieldName + " ";
        switch (metricsObjectType) {
            case "last":
                sb.append(MetricsObjectType.LAST.getVal());
                sb.append(tempSql);
                break;
            case "first":
                sb.append(MetricsObjectType.FIRST.getVal());
                sb.append(tempSql);
                break;
            case "max":
                sb.append(MetricsObjectType.MAX.getVal());
                sb.append(tempSql);
                break;
            case "min":
                sb.append(MetricsObjectType.MIN.getVal());
                sb.append(tempSql);
                break;
            case "count":
                sb.append(MetricsObjectType.COUNT.getVal());
                sb.append(tempSql);
                break;
            default:
                sb.append(fieldName);
                break;
        }
        sb.append(" from ");
        sb.append(MetricsInfluxdbReporter.METRICS_MEASUREMENT);
        sb.append(" where category='");
        sb.append(category);
        sb.append("' and entity='");
        sb.append(entity);
        sb.append("'");
        if (!MapUtils.isEmpty(tags)) {
            filterTags(tags).forEach((k, v) -> {
                sb.append(" and ");
                sb.append(k);
                sb.append("='");
                sb.append(v);
                sb.append("'");
            });
        }
        if (filter != null) {
            sb.append(filter);
        }
        sb.append(" order by time desc limit 1;");
        return sb.toString();
    }

    private static Meter registerMeterIfAbsent(String name, String category, String entity, Map<String, String> tags) {
        final String metricName = metricName(name, category, entity, tags);
        if (!meters.containsKey(metricName)) {
            synchronized (meters) {
                if (!meters.containsKey(metricName)) {
                    final Meter metric = MetricsController.getDefaultMetricRegistry().meter(metricName);
                    meters.put(metricName, metric);
                    logger.trace("ke.metrics register meter: {}", metricName);
                }
            }
        }
        return meters.get(metricName);
    }

    private static Histogram registerHistogramIfAbsent(String name, String category, String entity,
            Map<String, String> tags) {
        final String metricName = metricName(name, category, entity, tags);
        if (!histograms.containsKey(metricName)) {
            synchronized (histograms) {
                if (!histograms.containsKey(metricName)) {
                    final Histogram metric = MetricsController.getDefaultMetricRegistry().histogram(metricName);
                    histograms.put(metricName, metric);
                    logger.trace("ke.metrics register histogram: {}", metricName);
                }
            }
        }
        return histograms.get(metricName);
    }

    private static void doRemove(final String metricNameSuffix, final Iterator<String> it,
            final MetricRegistry registry) {
        // replace with removeIf
        while (it.hasNext()) {
            //some1:k1=v1,k2=v2,k3=v3,...
            final String metricName = it.next();
            try {
                String[] arr = metricName.split(":", 2);
                if (metricNameSuffix.equals(arr[1]) || arr[1].startsWith(metricNameSuffix + ",")) {
                    registry.remove(metricName);
                    it.remove();
                    logger.trace("ke.metrics remove metric: {}", metricName);
                }
            } catch (Exception e) {
                logger.warn("ke.metrics remove metric: {} {}", metricName, e.getMessage());
            }
        }
    }

    public static Map<String, String> getHostTagMap(String entity) {
        String host = AddressUtil.getZkLocalInstance();
        return getHostTagMap(host, entity);
    }

    public static Map<String, String> getHostTagMap(String host, String entity) {
        StringBuilder sb = new StringBuilder(host);
        sb.append("-").append(entity);
        Map<String, String> tags = Maps.newHashMap();
        tags.put(MetricsTag.HOST.getVal(), sb.toString());
        return tags;
    }
}

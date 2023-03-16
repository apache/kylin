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

package org.apache.kylin.common.metric;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsController;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.metrics.MetricsObject;
import org.apache.kylin.common.metrics.MetricsObjectType;
import org.apache.kylin.common.metrics.gauges.QueryRatioGauge;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;

import org.apache.kylin.shaded.influxdb.org.influxdb.dto.QueryResult;

public class InfluxMetricsTest extends NLocalFileMetadataTestCase {

    private static Logger logger = LoggerFactory.getLogger(InfluxMetricsTest.class);

    @Before
    public void setUp() throws Exception {
        NLocalFileMetadataTestCase.staticCreateTestMetadata();
    }

    @Test
    public void metricObjectTest() {
        String fieldName = "query_total_times";
        String category = "project";
        String entity = "test_project";
        Map<String, String> tags = new HashMap();
        tags.put("host", "localhost:8080-test_project");
        MetricsObject metricsObject = new MetricsObject(fieldName, category, entity, tags);
        Assert.assertEquals("query_total_times,project,test_project,{host=localhost:8080-test_project}", metricsObject.toString());
        Assert.assertFalse(metricsObject.isInitStatus());
        metricsObject = new MetricsObject(fieldName, category, entity, null);
        Assert.assertEquals("query_total_times,project,test_project,", metricsObject.toString());
    }

    @Test
    public void metricInfluxQueryTest() {
        String fieldName = "query_total_times";
        String category = "project";
        String entity = "test_01";
        Map<String, String> tags = new HashMap();
        tags.put("host", "localhost:8080-test_01");
        MetricsObject metricsObject = new MetricsObject(fieldName, category, entity, tags);
        try {
            Object firstInfluxQuerySql = MetricsGroup.getValFromInfluxQuerySql(MetricsObjectType.FIRST.getVal(), metricsObject, null);
            Object lastInfluxQuerySql = MetricsGroup.getValFromInfluxQuerySql(MetricsObjectType.LAST.getVal(), metricsObject, null);
            Object maxInfluxQuerySql = MetricsGroup.getValFromInfluxQuerySql(MetricsObjectType.MAX.getVal(), metricsObject, null);
            Object minInfluxQuerySql = MetricsGroup.getValFromInfluxQuerySql(MetricsObjectType.MIN.getVal(), metricsObject, null);
            Object countInfluxQuerySql = MetricsGroup.getValFromInfluxQuerySql(MetricsObjectType.COUNT.getVal(), metricsObject, null);
            Object defaultInfluxQuerySql = MetricsGroup.getValFromInfluxQuerySql("", metricsObject, null);
            Assert.assertEquals("select first(query_total_times) as query_total_times  "
                            + "from system_metric where category='project' and entity='test_01' and host='localhost:8080-test_01' order by time desc limit 1;",
                    firstInfluxQuerySql);
            Assert.assertEquals("select last(query_total_times) as query_total_times  "
                            + "from system_metric where category='project' and entity='test_01' and host='localhost:8080-test_01' order by time desc limit 1;",
                    lastInfluxQuerySql);
            Assert.assertEquals("select max(query_total_times) as query_total_times  "
                            + "from system_metric where category='project' and entity='test_01' and host='localhost:8080-test_01' order by time desc limit 1;",
                    maxInfluxQuerySql);
            Assert.assertEquals("select min(query_total_times) as query_total_times  "
                            + "from system_metric where category='project' and entity='test_01' and host='localhost:8080-test_01' order by time desc limit 1;",
                    minInfluxQuerySql);
            Assert.assertEquals("select count(query_total_times) as query_total_times  "
                            + "from system_metric where category='project' and entity='test_01' and host='localhost:8080-test_01' order by time desc limit 1;",
                    countInfluxQuerySql);
            Assert.assertEquals("select query_total_times "
                    + "from system_metric where category='project' and entity='test_01' and host='localhost:8080-test_01' order by time desc limit 1;",
                    defaultInfluxQuerySql);
            firstInfluxQuerySql = MetricsGroup.getValFromInfluxQuerySql(MetricsObjectType.FIRST.getVal(), metricsObject, " and 1=1 ");
            lastInfluxQuerySql = MetricsGroup.getValFromInfluxQuerySql(MetricsObjectType.LAST.getVal(), metricsObject, " and 1=1 ");
            maxInfluxQuerySql = MetricsGroup.getValFromInfluxQuerySql(MetricsObjectType.MAX.getVal(), metricsObject, " and 1=1 ");
            minInfluxQuerySql = MetricsGroup.getValFromInfluxQuerySql(MetricsObjectType.MIN.getVal(), metricsObject, " and 1=1 ");
            countInfluxQuerySql = MetricsGroup.getValFromInfluxQuerySql(MetricsObjectType.COUNT.getVal(), metricsObject, " and 1=1 ");
            defaultInfluxQuerySql = MetricsGroup.getValFromInfluxQuerySql("", metricsObject, " and 1=1 ");
            Assert.assertEquals("select first(query_total_times) as query_total_times  "
                            + "from system_metric where category='project' and entity='test_01' and host='localhost:8080-test_01' and 1=1  order by time desc limit 1;",
                    firstInfluxQuerySql);
            Assert.assertEquals("select last(query_total_times) as query_total_times  "
                            + "from system_metric where category='project' and entity='test_01' and host='localhost:8080-test_01' and 1=1  order by time desc limit 1;",
                    lastInfluxQuerySql);
            Assert.assertEquals("select max(query_total_times) as query_total_times  "
                            + "from system_metric where category='project' and entity='test_01' and host='localhost:8080-test_01' and 1=1  order by time desc limit 1;",
                    maxInfluxQuerySql);
            Assert.assertEquals("select min(query_total_times) as query_total_times  "
                            + "from system_metric where category='project' and entity='test_01' and host='localhost:8080-test_01' and 1=1  order by time desc limit 1;",
                    minInfluxQuerySql);
            Assert.assertEquals("select count(query_total_times) as query_total_times  "
                            + "from system_metric where category='project' and entity='test_01' and host='localhost:8080-test_01' and 1=1  order by time desc limit 1;",
                    countInfluxQuerySql);
            Assert.assertEquals("select query_total_times "
                            + "from system_metric where category='project' and entity='test_01' and host='localhost:8080-test_01' and 1=1  order by time desc limit 1;",
                    defaultInfluxQuerySql);

            metricsObject = new MetricsObject(fieldName, category, entity, new HashedMap());
            firstInfluxQuerySql = MetricsGroup.getValFromInfluxQuerySql(MetricsObjectType.FIRST.getVal(), metricsObject, " and 1=1 ");
            lastInfluxQuerySql = MetricsGroup.getValFromInfluxQuerySql(MetricsObjectType.LAST.getVal(), metricsObject, " and 1=1 ");
            maxInfluxQuerySql = MetricsGroup.getValFromInfluxQuerySql(MetricsObjectType.MAX.getVal(), metricsObject, " and 1=1 ");
            minInfluxQuerySql = MetricsGroup.getValFromInfluxQuerySql(MetricsObjectType.MIN.getVal(), metricsObject, " and 1=1 ");
            countInfluxQuerySql = MetricsGroup.getValFromInfluxQuerySql(MetricsObjectType.COUNT.getVal(), metricsObject, " and 1=1 ");
            defaultInfluxQuerySql = MetricsGroup.getValFromInfluxQuerySql("", metricsObject, " and 1=1 ");
            Assert.assertEquals("select first(query_total_times) as query_total_times  "
                            + "from system_metric where category='project' and entity='test_01' and 1=1  order by time desc limit 1;",
                    firstInfluxQuerySql);
            Assert.assertEquals("select last(query_total_times) as query_total_times  "
                            + "from system_metric where category='project' and entity='test_01' and 1=1  order by time desc limit 1;",
                    lastInfluxQuerySql);
            Assert.assertEquals("select max(query_total_times) as query_total_times  "
                            + "from system_metric where category='project' and entity='test_01' and 1=1  order by time desc limit 1;",
                    maxInfluxQuerySql);
            Assert.assertEquals("select min(query_total_times) as query_total_times  "
                            + "from system_metric where category='project' and entity='test_01' and 1=1  order by time desc limit 1;",
                    minInfluxQuerySql);
            Assert.assertEquals("select count(query_total_times) as query_total_times  "
                            + "from system_metric where category='project' and entity='test_01' and 1=1  order by time desc limit 1;",
                    countInfluxQuerySql);
            Assert.assertEquals("select query_total_times "
                            + "from system_metric where category='project' and entity='test_01' and 1=1  order by time desc limit 1;",
                    defaultInfluxQuerySql);

        } catch (Exception e) {
            logger.error("MetricInfluxQueryTest failed metricsObject:{}", metricsObject.toString(), e);
            Assert.assertTrue(metricsObject.isInitStatus());
        }
    }

    @Test
    public void metricGroupTest() {
        String fieldName = "query_total_times";
        String category = "project";
        String entity = "test_02";
        Map<String, String> tags = new HashMap();
        tags.put("host", "localhost:8080-test_02");
        MetricsObject metricsObject = new MetricsObject(fieldName, category, entity, tags);
        String cacheKey = metricsObject.toString();
        MetricsGroup.cacheInfluxMetricStatusMap.put(cacheKey, metricsObject);
        MetricsGroup.monitorRegisterMetrics();
        metricsObject = MetricsGroup.cacheInfluxMetricStatusMap.get(cacheKey);
        Counter counter = MetricsGroup.counters.get(MetricsGroup.metricName(fieldName, category, entity, tags));
        Assert.assertTrue(metricsObject.isInitStatus());
        Assert.assertEquals(9999999999L, counter.getCount());
        MetricsGroup.monitorRegisterMetrics();
        String metricName = MetricsGroup.metricName(fieldName, category, entity, new HashedMap());
        Assert.assertEquals("query_total_times:category=project,entity=test_02", metricName);
        Map newTags = new HashedMap();
        newTags.put("category", "1");
        newTags.put("entity", "2");
        metricName = MetricsGroup.metricName(fieldName, category, entity, newTags);
        Assert.assertEquals("query_total_times:category=project,entity=test_02", metricName);
    }

    @Test
    public void initRestoreMetricsTest() {
        String fieldName = "query_total_times";
        String category = "project";
        String entity = "test_03";
        Map<String, String> tags = new HashMap();
        tags.put("host", "localhost:8080-test_03");
        MetricsObject metricsObject = new MetricsObject(fieldName, category, entity, tags);
        long result = MetricsGroup.tryRestoreCounter(fieldName, category, entity, tags);
        Assert.assertEquals(9999999999L, result);
        String cacheKey = metricsObject.toString();
        metricsObject = MetricsGroup.cacheInfluxMetricStatusMap.get(cacheKey);
        Assert.assertTrue(metricsObject.isInitStatus());
    }

    @Test
    public void counterIncTest() {
        String entity = "test_04";
        Map<String, String> tags = new HashMap();
        tags.put("host", "localhost:8080-test_04");
        boolean result = MetricsGroup.counterInc(MetricsName.QUERY, MetricsCategory.HOST, entity, tags);
        Assert.assertTrue(result);
        Counter counter = MetricsGroup.counters.get("query_total_times:category=host,entity=test_04,host=localhost:8080-test_04");
        Assert.assertEquals(10000000000L, counter.getCount());
        result = MetricsGroup.counterInc(null, MetricsCategory.HOST, entity, tags, 3);
        Assert.assertFalse(result);
        result = MetricsGroup.counterInc(MetricsName.QUERY, MetricsCategory.HOST, entity, tags, -3);
        Assert.assertFalse(result);
        result = MetricsGroup.counterInc(MetricsName.QUERY, MetricsCategory.HOST, entity);
        Assert.assertTrue(result);
        counter = MetricsGroup.counters.get("query_total_times:category=host,entity=test_04");
        Assert.assertEquals(10000000000L, counter.getCount());
        result = MetricsGroup.counterInc(MetricsName.QUERY, MetricsCategory.HOST, entity, 2);
        Assert.assertTrue(result);
    }

    @Test
    public void histogramUpdateTest() {
        String entity = "test_05";
        Map<String, String> tags = new HashMap();
        tags.put("host", "localhost:8080-test_05");
        boolean result = MetricsGroup.histogramUpdate(MetricsName.QUERY, MetricsCategory.HOST, entity, 10);
        Assert.assertTrue(result);
        result = MetricsGroup.histogramUpdate(null, MetricsCategory.HOST, entity, 10);
        Assert.assertFalse(result);
        Histogram histogram = MetricsGroup.histograms.get("query_total_times:category=host,entity=test_05");
        Assert.assertEquals(1, histogram.getCount());
        Assert.assertEquals(10, histogram.getSnapshot().getValues()[0]);
        result = MetricsGroup.histogramUpdate(MetricsName.QUERY, MetricsCategory.HOST, entity, -10);
        Assert.assertFalse(result);
        result = MetricsGroup.histogramUpdate(MetricsName.QUERY, MetricsCategory.HOST, entity, tags, 20);
        Assert.assertTrue(result);
        histogram = MetricsGroup.histograms.get("query_total_times:category=host,entity=test_05,host=localhost:8080-test_05");
        Assert.assertEquals(20, histogram.getSnapshot().getValues()[0]);
    }

    @Test
    public void meterMarkTest() {
        String entity = "test_06";
        Map<String, String> tags = new HashMap();
        tags.put("host", "localhost:8080-test_06");
        boolean result = MetricsGroup.meterMark(MetricsName.QUERY, MetricsCategory.HOST, entity);
        Assert.assertTrue(result);
        result = MetricsGroup.meterMark(null, MetricsCategory.HOST, entity);
        Assert.assertFalse(result);
        Meter meter = MetricsGroup.meters.get("query_total_times:category=host,entity=test_06");
        Assert.assertEquals(1, meter.getCount());
        result = MetricsGroup.meterMark(MetricsName.QUERY, MetricsCategory.HOST, entity, tags);
        Assert.assertTrue(result);
        meter = MetricsGroup.meters.get("query_total_times:category=host,entity=test_06,host=localhost:8080-test_06");
        Assert.assertEquals(1, meter.getCount());
    }

    @Test
    public void removeGlobalMetricsTest() {
        clearRegistry();
        MetricRegistry registry = MetricsController.getDefaultMetricRegistry();
        String metricName = "project:category=global,entity=global,host=localhost:8080-test_0701";
        Counter counter = new Counter();
        counter.inc(6);
        registry.register(metricName, counter);
        MetricsGroup.counters.put(metricName, counter);

        String metricName2 = "project:category=global,entity=global,host=localhost:8080-test_0702";
        Histogram histogram = MetricsController.getDefaultMetricRegistry().histogram(metricName2);
        counter.inc(7);
        MetricsGroup.histograms.put(metricName2, histogram);

        String metricName3 = "project:category=global,entity=global,host=localhost:8080-test_0703";
        Meter meter = new Meter();
        counter.inc(8);
        registry.register(metricName3, meter);
        MetricsGroup.meters.put(metricName3, meter);

        Assert.assertEquals(3, registry.getMetrics().size());
        Assert.assertNotNull(MetricsGroup.counters.get(metricName));
        Assert.assertNotNull(MetricsGroup.histograms.get(metricName2));
        Assert.assertNotNull(MetricsGroup.meters.get(metricName3));
        boolean result = MetricsGroup.removeGlobalMetrics();
        Assert.assertTrue(result);
        Assert.assertEquals(0, registry.getMetrics().size());
        Assert.assertNull(MetricsGroup.counters.get(metricName));
        Assert.assertNull(MetricsGroup.histograms.get(metricName2));
        Assert.assertNull(MetricsGroup.meters.get(metricName3));

        String metricName4 = "project,category=global,entity=global";
        Counter counter4 = new Counter();
        counter4.inc(1);
        registry.register(metricName4, counter4);
        MetricsGroup.counters.put(metricName4, counter4);
        MetricsGroup.removeGlobalMetrics();
    }

    @Test
    public void removeProjectMetricsTest() {
        clearRegistry();
        MetricRegistry registry = MetricsController.getDefaultMetricRegistry();
        String metricName = "test_01:category=project,entity=test_project,host=localhost:8080-test_0701";
        Counter counter = new Counter();
        counter.inc(1);
        registry.register(metricName, counter);
        MetricsGroup.counters.put(metricName, counter);

        String metricName2 = "test_02:category=project,entity=test_project,host=localhost:8080-test_0702";
        Histogram histogram = MetricsController.getDefaultMetricRegistry().histogram(metricName2);
        counter.inc(2);
        MetricsGroup.histograms.put(metricName2, histogram);

        String metricName3 = "test_03:category=project,entity=test_project,host=localhost:8080-test_0703";
        Meter meter = new Meter();
        counter.inc(3);
        registry.register(metricName3, meter);
        MetricsGroup.meters.put(metricName3, meter);

        Assert.assertEquals(3, registry.getMetrics().size());
        Assert.assertNotNull(MetricsGroup.counters.get(metricName));
        Assert.assertNotNull(MetricsGroup.histograms.get(metricName2));
        Assert.assertNotNull(MetricsGroup.meters.get(metricName3));
        boolean result = MetricsGroup.removeProjectMetrics("test_project");
        Assert.assertTrue(result);
        Assert.assertEquals(0, registry.getMetrics().size());
        Assert.assertNull(MetricsGroup.counters.get(metricName));
        Assert.assertNull(MetricsGroup.histograms.get(metricName2));
        Assert.assertNull(MetricsGroup.meters.get(metricName3));
        try {
            MetricsGroup.removeProjectMetrics(null);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void removeModelMetricsTest() {
        clearRegistry();
        MetricRegistry registry = MetricsController.getDefaultMetricRegistry();
        String metricName = "test_01:category=project,entity=test_project,model=123,host=localhost:8080-test_0701";
        Counter counter = new Counter();
        counter.inc(1);
        registry.register(metricName, counter);
        MetricsGroup.counters.put(metricName, counter);

        String metricName2 = "test_02:category=project,entity=test_project,model=123,host=localhost:8080-test_0702";
        Histogram histogram = MetricsController.getDefaultMetricRegistry().histogram(metricName2);
        counter.inc(2);
        MetricsGroup.histograms.put(metricName2, histogram);

        String metricName3 = "test_03:category=project,entity=test_project,model=123,host=localhost:8080-test_0703";
        Meter meter = new Meter();
        counter.inc(3);
        registry.register(metricName3, meter);
        MetricsGroup.meters.put(metricName3, meter);

        Assert.assertEquals(3, registry.getMetrics().size());
        Assert.assertNotNull(MetricsGroup.counters.get(metricName));
        Assert.assertNotNull(MetricsGroup.histograms.get(metricName2));
        Assert.assertNotNull(MetricsGroup.meters.get(metricName3));
        boolean result = MetricsGroup.removeModelMetrics("test_project", "123");
        Assert.assertTrue(result);
        try {
            MetricsGroup.removeModelMetrics(null, "123");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
        try {
            MetricsGroup.removeModelMetrics("test_project", null);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
        Assert.assertEquals(0, registry.getMetrics().size());
        Assert.assertNull(MetricsGroup.counters.get(metricName));
        Assert.assertNull(MetricsGroup.histograms.get(metricName2));
        Assert.assertNull(MetricsGroup.meters.get(metricName3));

    }

    @Test
    public void newGaugeTest() {
        clearRegistry();
        Map<String, String> tags = new HashMap();
        tags.put("host", "localhost:8080-test_06");
        String projectName = "test_project";
        boolean result = MetricsGroup.newCounter(MetricsName.QUERY, MetricsCategory.PROJECT, projectName, tags);
        Assert.assertTrue(result);
        result = MetricsGroup.newCounter(MetricsName.QUERY, MetricsCategory.PROJECT, projectName);
        Assert.assertTrue(result);
        Counter denominator = MetricsGroup.getCounter(MetricsName.QUERY, MetricsCategory.PROJECT, projectName, tags);
        result = MetricsGroup.newCounter(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, projectName, tags);
        Assert.assertTrue(result);
        Counter numerator = MetricsGroup.getCounter(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, projectName, tags);
        result = MetricsGroup.newGauge(MetricsName.QUERY_LT_1S_RATIO, MetricsCategory.PROJECT, projectName, tags,
                new QueryRatioGauge(numerator, denominator));
        Assert.assertTrue(result);
        Assert.assertEquals(3, MetricsGroup.counters.size());
        Gauge gauge = MetricsGroup.getGauge(MetricsName.QUERY_LT_1S_RATIO, MetricsCategory.PROJECT, projectName, tags);
        Assert.assertNotNull(gauge);
        result = MetricsGroup.newGauge(MetricsName.QUERY_LT_1S_RATIO, MetricsCategory.PROJECT, projectName,
                new QueryRatioGauge(numerator, denominator));
        Assert.assertTrue(result);
        result = MetricsGroup.newGauge(MetricsName.QUERY_LT_1S_RATIO, MetricsCategory.PROJECT, projectName,
                new QueryRatioGauge(numerator, denominator));
        Assert.assertFalse(result);
    }

    @Test
    public void newHistogramTest() {
        clearRegistry();
        Map<String, String> tags = new HashMap();
        tags.put("host", "localhost:8080-test_013");
        String projectName = "test_project";
        boolean result = MetricsGroup.newHistogram(MetricsName.QUERY, MetricsCategory.PROJECT, projectName, tags);
        Assert.assertTrue(result);
        result = MetricsGroup.newHistogram(MetricsName.QUERY, MetricsCategory.PROJECT, projectName);
        Assert.assertTrue(result);
        result = MetricsGroup.newHistogram(null, MetricsCategory.PROJECT, projectName);
        Assert.assertFalse(result);
    }

    @Test
    public void newMeterTest() {
        clearRegistry();
        Map<String, String> tags = new HashMap();
        tags.put("host", "localhost:8080-test_013");
        String projectName = "test_project";
        boolean result = MetricsGroup.newMeter(MetricsName.QUERY, MetricsCategory.PROJECT, projectName);
        Assert.assertTrue(result);
        result = MetricsGroup.newMeter(MetricsName.QUERY, MetricsCategory.PROJECT, projectName);
        Assert.assertTrue(result);
    }

    @Test
    public void newMetricSetTest() {
        clearRegistry();
        String entity = "test_project";
        MetricSet metricSet = new GarbageCollectorMetricSet();
        MetricsGroup.newMetricSet(MetricsName.QUERY, MetricsCategory.PROJECT, entity, metricSet);
        Assert.assertEquals(metricSet.getMetrics().size(), MetricsGroup.gauges.size());
        MetricRegistry registry = MetricsController.getDefaultMetricRegistry();
        registry.register("jvmMetrics", metricSet);
        MetricsGroup.newMetricSet(MetricsName.QUERY, MetricsCategory.PROJECT, entity, registry);
        Assert.assertEquals(15, MetricsGroup.gauges.size());
    }

    @Test
    public void registerProjectMetricsTest() {
        clearRegistry();
        boolean result = MetricsGroup.registerProjectMetrics("test_project", "localhost:8080-test_10");
        Assert.assertTrue(result);
        Assert.assertEquals(28, MetricsGroup.counters.size());
        Assert.assertEquals(4, MetricsGroup.histograms.size());
        Assert.assertEquals(5, MetricsGroup.meters.size());
        try {
            MetricsGroup.registerProjectMetrics(null, "localhost:8080-test_10");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void tryRestoreExceptionCounterTest() {
        String fieldName = "query_total_times";
        String category = "project";
        String entity = "test_15";
        Map<String, String> tags = new HashMap();
        tags.put("host", "localhost:8080-test_15");
        MetricsObject metricsObject = new MetricsObject(fieldName, category, entity, tags);
        long result = MetricsGroup.tryRestoreExceptionCounter(metricsObject);
        Assert.assertEquals(0, result);
        MetricsGroup.cacheInfluxMetricStatusMap.put(metricsObject.toString(), metricsObject);
        result = MetricsGroup.tryRestoreExceptionCounter(metricsObject, null);
        Assert.assertEquals(-2, result);
    }

    @Test
    public void getResultFromSeriesTest() {
        try {
            String fieldName = "query_total_times";
            QueryResult queryResult = new QueryResult();
            List<QueryResult.Result> resultList = new ArrayList<>();
            queryResult.setResults(resultList);
            QueryResult.Result result = new QueryResult.Result();
            List<QueryResult.Series> seriesList = new ArrayList<>();
            QueryResult.Series series = new QueryResult.Series();
            series.setName("seriesName");
            List<String> columns = new ArrayList<>();
            List<List<Object>> values = new ArrayList<>();
            List<Object> valuesData = new ArrayList<>();
            valuesData.add(12);
            valuesData.add(13);
            values.add(valuesData);
            columns.add("testColumn");
            columns.add(fieldName);
            series.setColumns(columns);
            series.setValues(values);
            seriesList.add(series);
            result.setSeries(new ArrayList<>());
            resultList.add(result);
            queryResult.setResults(resultList);
            long resultFromSeries = MetricsGroup.getResultFromSeries(queryResult, fieldName);
            Assert.assertEquals(0, resultFromSeries);
            result.setSeries(seriesList);
            resultFromSeries = MetricsGroup.getResultFromSeries(queryResult, fieldName);
            Assert.assertEquals(13, resultFromSeries);
        } catch (Exception e) {
            logger.warn("GetResultFromSeriesTest error", e);
        }
    }

    public void clearRegistry() {
        MetricRegistry registry = MetricsController.getDefaultMetricRegistry();
        Map<String, Metric> metricsMap = registry.getMetrics();
        Iterator<String> it = metricsMap.keySet().iterator();
        while (it.hasNext()) {
            registry.remove(it.next());
        }
        it = MetricsGroup.counters.keySet().iterator();
        while (it.hasNext()) {
            MetricsGroup.counters.remove(it.next());
        }
        it = MetricsGroup.histograms.keySet().iterator();
        while (it.hasNext()) {
            MetricsGroup.histograms.remove(it.next());
        }
        it = MetricsGroup.meters.keySet().iterator();
        while (it.hasNext()) {
            MetricsGroup.meters.remove(it.next());
        }
        MetricsGroup.gauges.removeAll(MetricsGroup.gauges);
    }

}

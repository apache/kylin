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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.metrics.prometheus.PrometheusMetrics;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.rest.config.initialize.QueryMetricsListener;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.val;

public class QueryMetricsListenerTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private QueryMetricsListener queryMetricsListener;
    @InjectMocks
    private QueryService queryService;

    @Before
    public void setup() {
        createTestMetadata();
        getTestConfig().setProperty("kylin.monitor.enabled", "true");
        queryMetricsListener = Mockito.spy(new QueryMetricsListener());
        queryService = Mockito.spy(new QueryService());
    }

    @Test
    public void testSqlsFormat() {
        List<String> sqls = Lists.newArrayList("select * from A", "select A.a, B.b from A join B on A.a2=B.b2",
                "Select sum(a), b from A group by b", "select * from A as c limit 1");

        // see https://olapio.atlassian.net/browse/KE-42029
        // Calcite 1.30 SQL format changes, not exceptions do not need to be adjusted
        List<String> expectedFormattedSqls = Lists.newArrayList("SELECT *\nFROM \"A\"",
                "SELECT \"A\".\"A\",\n  \"B\".\"B\"\nFROM \"A\"\n  INNER JOIN \"B\" ON \"A\".\"A2\" = \"B\".\"B2\"",
                "SELECT SUM(\"A\"),\n  \"B\"\nFROM \"A\"\nGROUP BY \"B\"",
                "SELECT *\nFROM \"A\" AS \"C\"\nLIMIT 1");
        val formated = queryService.format(sqls);

        Assert.assertEquals(sqls.size(), formated.size());
        for (int n = 0; n < sqls.size(); n++) {
            Assert.assertEquals(expectedFormattedSqls.get(n), formated.get(n));
        }
    }

    @Test
    public void testAliasLengthMaxThanConfig() {
        //To check if the config kylin.model.dimension-measure-name.max-length worked for SqlParser
        List<String> sqls = Lists.newArrayList("select A.a as AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA from A");

        // see https://olapio.atlassian.net/browse/KE-42029
        // Calcite 1.30 SQL format changes, not exceptions do not need to be adjusted
        List<String> expectedFormattedSqls = Lists.newArrayList("SELECT "
                + "\"A\".\"A\" AS \"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\"\n" + "FROM \"A\"");

        val formated = queryService.format(sqls);

        Assert.assertEquals(sqls.size(), formated.size());
        for (int n = 0; n < sqls.size(); n++) {
            Assert.assertEquals(expectedFormattedSqls.get(n), formated.get(n));
        }
    }

    private long getCounterCount(MetricsName name, MetricsCategory category, String entity, Map<String, String> tags) {
        val counter = MetricsGroup.getCounter(name, category, entity, tags);
        return counter == null ? 0 : counter.getCount();
    }

    @Test
    public void testUpdateQueryTimeMetrics() {
        long a = getCounterCount(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, "default", new HashMap<>());
        long b = getCounterCount(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, "default", new HashMap<>());
        long c = getCounterCount(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, "default", new HashMap<>());
        long d = getCounterCount(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, "default", new HashMap<>());
        long e = getCounterCount(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, "default", new HashMap<>());
        queryMetricsListener.updateQueryTimeMetrics(123, "default", new HashMap<>());
        Assert.assertEquals(a + 1,
                getCounterCount(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(b,
                getCounterCount(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(c,
                getCounterCount(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(d,
                getCounterCount(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(e,
                getCounterCount(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, "default", new HashMap<>()));
        queryMetricsListener.updateQueryTimeMetrics(1123, "default", new HashMap<>());
        Assert.assertEquals(a + 1,
                getCounterCount(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(b + 1,
                getCounterCount(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(c,
                getCounterCount(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(d,
                getCounterCount(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(e,
                getCounterCount(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, "default", new HashMap<>()));
        queryMetricsListener.updateQueryTimeMetrics(3123, "default", new HashMap<>());
        Assert.assertEquals(a + 1,
                getCounterCount(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(b + 1,
                getCounterCount(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(c + 1,
                getCounterCount(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(d,
                getCounterCount(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(e,
                getCounterCount(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, "default", new HashMap<>()));
        queryMetricsListener.updateQueryTimeMetrics(5123, "default", new HashMap<>());
        Assert.assertEquals(a + 1,
                getCounterCount(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(b + 1,
                getCounterCount(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(c + 1,
                getCounterCount(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(d + 1,
                getCounterCount(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(e,
                getCounterCount(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, "default", new HashMap<>()));
        queryMetricsListener.updateQueryTimeMetrics(10123, "default", new HashMap<>());
        Assert.assertEquals(a + 1,
                getCounterCount(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(b + 1,
                getCounterCount(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(c + 1,
                getCounterCount(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(d + 1,
                getCounterCount(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(e + 1,
                getCounterCount(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, "default", new HashMap<>()));
    }

    @Test
    public void testRecordQueryPrometheusMetric() {
        QueryMetrics queryMetrics = Mockito.mock(QueryMetrics.class);
        QueryHistoryInfo queryHistoryInfo = Mockito.mock(QueryHistoryInfo.class);
        Mockito.when(queryMetrics.getQueryHistoryInfo()).thenReturn(queryHistoryInfo);
        Mockito.when(queryHistoryInfo.isExactlyMatch()).thenReturn(true);
        Mockito.when(queryMetrics.getProjectName()).thenReturn("project");
        Mockito.when(queryMetrics.isIndexHit()).thenReturn(false);
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        val modelManager = NDataModelManager.getInstance(getTestConfig(), "project");
        Mockito.when(queryMetrics.getRealizationMetrics()).thenReturn(Collections.emptyList());
        KylinConfig.getInstanceFromEnv().setProperty("kylin.metrics.prometheus-enabled", "false");
        queryMetricsListener.recordQueryPrometheusMetric(queryMetrics, modelManager, meterRegistry);
        Collection<Meter> meters1 = meterRegistry.getMeters();
        Assert.assertEquals(0, meters1.size());

        KylinConfig.getInstanceFromEnv().setProperty("kylin.metrics.prometheus-enabled", "true");
        queryMetricsListener.recordQueryPrometheusMetric(queryMetrics, modelManager, meterRegistry);
        Collection<Meter> meters2 = meterRegistry.find(PrometheusMetrics.QUERY_SECONDS.getValue()).meters();
        Assert.assertEquals(1, meters2.size());

        Mockito.when(queryMetrics.isIndexHit()).thenReturn(true);
        Mockito.when(queryMetrics.isSucceed()).thenReturn(true);
        queryMetricsListener.recordQueryPrometheusMetric(queryMetrics, modelManager, meterRegistry);
        Collection<Meter> meters3 = meterRegistry.find(PrometheusMetrics.QUERY_SCAN_BYTES.getValue()).meters();
        Collection<Meter> meters4 = meterRegistry.find(PrometheusMetrics.QUERY_JOBS.getValue()).meters();
        Collection<Meter> meters5 = meterRegistry.find(PrometheusMetrics.QUERY_STAGES.getValue()).meters();
        Collection<Meter> meters6 = meterRegistry.find(PrometheusMetrics.QUERY_TASKS.getValue()).meters();
        Collection<Meter> meters7 = meterRegistry.find(PrometheusMetrics.QUERY_RESULT_ROWS.getValue()).meters();
        Assert.assertEquals(1, meters3.size());
        Assert.assertEquals(1, meters4.size());
        Assert.assertEquals(1, meters5.size());
        Assert.assertEquals(1, meters6.size());
        Assert.assertEquals(1, meters7.size());
        Collection<Meter> meters8 = meterRegistry.find(PrometheusMetrics.QUERY_SECONDS.getValue()).meters();
        Assert.assertNotEquals(0, meters8.size());
    }
}

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

package org.apache.kylin.rest.config.initialize;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.constant.Constant;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.metrics.MetricsTag;
import org.apache.kylin.common.metrics.prometheus.PrometheusMetrics;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.eventbus.Subscribe;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.query.QueryMetricsContext;
import org.apache.kylin.rest.util.SpringContext;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.val;

public class QueryMetricsListener {

    @Subscribe
    public void recordMetric(QueryMetrics queryMetric) {
        String project = queryMetric.getProjectName();
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        Map<String, String> tags = Maps.newHashMap();
        tags.put(MetricsTag.HOST.getVal(), queryMetric.getServer().concat("-").concat(project));

        MetricsGroup.counterInc(MetricsName.QUERY, MetricsCategory.PROJECT, project, tags);

        updateQueryTimeMetrics(queryMetric.getQueryDuration(), project, tags);
        updateQueryTypeMetrics(queryMetric, project, tags);

        MetricsGroup.counterInc(MetricsName.QUERY_HOST, MetricsCategory.HOST, queryMetric.getServer());
        MetricsGroup.counterInc(MetricsName.QUERY_SCAN_BYTES_HOST, MetricsCategory.HOST, queryMetric.getServer(),
                queryMetric.getTotalScanBytes());

        MetricsGroup.histogramUpdate(MetricsName.QUERY_LATENCY, MetricsCategory.PROJECT, queryMetric.getProjectName(),
                tags, queryMetric.getQueryDuration());
        MetricsGroup.histogramUpdate(MetricsName.QUERY_TIME_HOST, MetricsCategory.HOST, queryMetric.getServer(),
                queryMetric.getQueryDuration());

        MetricsGroup.histogramUpdate(MetricsName.QUERY_SCAN_BYTES, MetricsCategory.PROJECT, project, tags,
                queryMetric.getTotalScanBytes());

        recordQueryPrometheusMetric(queryMetric, modelManager, SpringContext.getBean(MeterRegistry.class));

    }

    public void recordQueryPrometheusMetric(QueryMetrics queryMetric, NDataModelManager modelManager,
            MeterRegistry meterRegistry) {
        if (!KylinConfig.getInstanceFromEnv().isPrometheusMetricsEnabled()) {
            return;
        }
        Tags projectTag = Tags.of(MetricsTag.PROJECT.getVal(), queryMetric.getProjectName());
        DistributionSummary.builder(PrometheusMetrics.QUERY_SECONDS.getValue())
                .tags(MetricsTag.PUSH_DOWN.getVal(), queryMetric.isPushdown() + "", MetricsTag.CACHE.getVal(),
                        queryMetric.isCacheHit() + "", MetricsTag.HIT_INDEX.getVal(), queryMetric.isIndexHit() + "",
                        MetricsTag.HIT_EXACTLY_INDEX.getVal(), queryMetric.getQueryHistoryInfo().isExactlyMatch() + "",
                        MetricsTag.SUCCEED.getVal(), queryMetric.isSucceed() + "", MetricsTag.HIT_SNAPSHOT.getVal(),
                        queryMetric.isTableSnapshotUsed() + "", MetricsTag.PROJECT.getVal(),
                        queryMetric.getProjectName(), MetricsTag.CONSTANTS.getVal(),
                        MetricsTag.CONSTANTS.getVal().equalsIgnoreCase(queryMetric.getEngineType()) + "")
                .distributionStatisticExpiry(Duration.ofDays(1))
                .sla(KylinConfig.getInstanceFromEnv().getMetricsQuerySlaSeconds()).description("Query duration")
                .register(meterRegistry).record(queryMetric.getQueryDuration() * 1.0 / 1000);

        if (queryMetric.isSucceed()) {
            DistributionSummary.builder(PrometheusMetrics.QUERY_RESULT_ROWS.getValue()).tags(projectTag)
                    .description("Number of rows returned by query").distributionStatisticExpiry(Duration.ofDays(1))
                    .register(meterRegistry).record(queryMetric.getResultRowCount());

            Counter.builder(PrometheusMetrics.QUERY_JOBS.getValue()).tags(projectTag)
                    .description("Number of spark job by query engine").register(meterRegistry)
                    .increment(queryMetric.getQueryJobCount());
            Counter.builder(PrometheusMetrics.QUERY_STAGES.getValue()).tags(projectTag)
                    .description("Number of spark stage by query engine").register(meterRegistry)
                    .increment(queryMetric.getQueryStageCount());
            Counter.builder(PrometheusMetrics.QUERY_TASKS.getValue()).tags(projectTag)
                    .description("Number of spark task by query engine").register(meterRegistry)
                    .increment(queryMetric.getQueryTaskCount());
        }

        if (queryMetric.isIndexHit()) {
            DistributionSummary.builder(PrometheusMetrics.QUERY_SCAN_BYTES.getValue()).tags(MetricsTag.MODEL.getVal(),
                    queryMetric.getRealizationMetrics().stream().map(e -> modelManager.getDataModelDesc(e.getModelId()))
                            .filter(Objects::nonNull).map(NDataModel::getAlias).collect(Collectors.joining(",")),
                    MetricsTag.PROJECT.getVal(), queryMetric.getProjectName())
                    .distributionStatisticExpiry(Duration.ofDays(1)).publishPercentiles(new double[] { 0.8, 0.9 })
                    .register(meterRegistry).record(queryMetric.getTotalScanBytes());
        }
    }

    private void updateQueryTypeMetrics(QueryMetrics queryMetrics, String project, Map<String, String> tags) {
        if (QueryHistory.QUERY_HISTORY_FAILED.equals(queryMetrics.getQueryStatus())) {
            MetricsGroup.counterInc(MetricsName.QUERY_FAILED, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.meterMark(MetricsName.QUERY_FAILED_RATE, MetricsCategory.PROJECT, project, tags);
        }

        if (queryMetrics.isPushdown()) {
            MetricsGroup.counterInc(MetricsName.QUERY_PUSH_DOWN, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.meterMark(MetricsName.QUERY_PUSH_DOWN_RATE, MetricsCategory.PROJECT, project, tags);
        }

        if ("CONSTANTS".equals(queryMetrics.getEngineType())) {
            MetricsGroup.counterInc(MetricsName.QUERY_CONSTANTS, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.meterMark(MetricsName.QUERY_CONSTANTS_RATE, MetricsCategory.PROJECT, project, tags);
        }

        if (queryMetrics.isTimeout()) {
            MetricsGroup.counterInc(MetricsName.QUERY_TIMEOUT, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.meterMark(MetricsName.QUERY_TIMEOUT_RATE, MetricsCategory.PROJECT, project, tags);
        }

        if (queryMetrics.isCacheHit()) {
            MetricsGroup.counterInc(MetricsName.QUERY_CACHE, MetricsCategory.PROJECT, project, tags);
        }

        if (queryMetrics.getRealizationMetrics() != null) {
            boolean hitAggIndex = queryMetrics.getRealizationMetrics().stream()
                    .anyMatch(realization -> realization != null
                            && QueryMetricsContext.AGG_INDEX.equals(realization.getIndexType()));
            boolean hitTableIndex = queryMetrics.getRealizationMetrics().stream()
                    .anyMatch(realization -> realization != null
                            && QueryMetricsContext.TABLE_INDEX.equals(realization.getIndexType()));
            if (hitAggIndex) {
                MetricsGroup.counterInc(MetricsName.QUERY_AGG_INDEX, MetricsCategory.PROJECT, project, tags);
            }
            if (hitTableIndex) {
                MetricsGroup.counterInc(MetricsName.QUERY_TABLE_INDEX, MetricsCategory.PROJECT, project, tags);
            }
        }
    }

    @VisibleForTesting
    public void updateQueryTimeMetrics(long duration, String project, Map<String, String> tags) {
        if (duration <= Constant.SECOND) {
            MetricsGroup.counterInc(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, project, tags);
        } else if (duration <= 3 * Constant.SECOND) {
            MetricsGroup.counterInc(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, project, tags);
        } else if (duration <= 5 * Constant.SECOND) {
            MetricsGroup.counterInc(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, project, tags);
        } else if (duration <= 10 * Constant.SECOND) {
            MetricsGroup.counterInc(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, project, tags);
        } else {
            MetricsGroup.counterInc(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.meterMark(MetricsName.QUERY_SLOW_RATE, MetricsCategory.PROJECT, project, tags);
        }
        MetricsGroup.counterInc(MetricsName.QUERY_TOTAL_DURATION, MetricsCategory.PROJECT, project, tags, duration);
    }

}

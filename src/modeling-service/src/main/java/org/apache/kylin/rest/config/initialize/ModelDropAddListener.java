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

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.event.ModelAddEvent;
import org.apache.kylin.common.event.ModelDropEvent;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.metrics.MetricsTag;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkContext;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.rest.util.ModelUtils;
import org.springframework.stereotype.Component;

import com.codahale.metrics.Gauge;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

import org.apache.kylin.guava30.shaded.common.eventbus.Subscribe;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class ModelDropAddListener {

    @Subscribe
    public void onDelete(ModelDropEvent modelDropEvent) {
        String project = modelDropEvent.getProject();
        String modelId = modelDropEvent.getModelId();
        String modelName = modelDropEvent.getModelName();
        UnitOfWorkContext context = UnitOfWork.get();
        context.doAfterUnit(() -> {
            log.debug("delete model {} in project {}", modelId, project);
            MetricsGroup.removeModelMetrics(project, modelId);
            if (KylinConfig.getInstanceFromEnv().isPrometheusMetricsEnabled()) {
                MetricsRegistry.removePrometheusModelMetrics(project, modelName);
            }
        });
    }

    @Subscribe
    public void onAdd(ModelAddEvent modelAddEvent) {
        String project = modelAddEvent.getProject();
        String modelId = modelAddEvent.getModelId();
        String modelAlias = modelAddEvent.getModelAlias();
        NDataflowManager dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        Map<String, String> tags = Maps.newHashMap();
        tags.put(MetricsTag.MODEL.getVal(), project.concat("-").concat(modelAlias));

        MetricsGroup.newGauge(MetricsName.MODEL_SEGMENTS, MetricsCategory.PROJECT, project, tags, new GaugeWrapper() {
            @Override
            public Long getResult() {
                NDataflow df = dfManager.getDataflow(modelId);
                return df == null ? 0L : df.getSegments().size();
            }
        });
        MetricsGroup.newGauge(MetricsName.MODEL_STORAGE, MetricsCategory.PROJECT, project, tags, new GaugeWrapper() {
            @Override
            public Long getResult() {
                NDataflow df = dfManager.getDataflow(modelId);
                return df == null ? 0L : df.getStorageBytesSize();
            }
        });
        MetricsGroup.newGauge(MetricsName.MODEL_LAST_QUERY_TIME, MetricsCategory.PROJECT, project, tags,
                new GaugeWrapper() {
                    @Override
                    public Long getResult() {
                        NDataflow df = dfManager.getDataflow(modelId);
                        return df == null ? 0L : df.getLastQueryTime();
                    }
                });
        MetricsGroup.newGauge(MetricsName.MODEL_QUERY_COUNT, MetricsCategory.PROJECT, project, tags,
                new GaugeWrapper() {
                    @Override
                    public Long getResult() {
                        NDataflow df = dfManager.getDataflow(modelId);
                        return df == null ? 0L : df.getQueryHitCount();
                    }
                });
        MetricsGroup.newGauge(MetricsName.MODEL_INDEX_NUM_GAUGE, MetricsCategory.PROJECT, project, tags,
                new GaugeWrapper() {
                    @Override
                    public Long getResult() {
                        NDataflow df = dfManager.getDataflow(modelId);
                        return df == null ? 0L : df.getIndexPlan().getAllLayouts().size();
                    }
                });

        MetricsGroup.newGauge(MetricsName.MODEL_EXPANSION_RATE_GAUGE, MetricsCategory.PROJECT, project, tags, () -> {
            NDataflow df = dfManager.getDataflow(modelId);
            return df == null ? (double) 0
                    : Double.parseDouble(
                            ModelUtils.computeExpansionRate(df.getStorageBytesSize(), df.getSourceBytesSize()));
        });

        MetricsGroup.newCounter(MetricsName.MODEL_BUILD_DURATION, MetricsCategory.PROJECT, project, tags);
        MetricsGroup.newCounter(MetricsName.MODEL_WAIT_DURATION, MetricsCategory.PROJECT, project, tags);
        MetricsGroup.newHistogram(MetricsName.MODEL_BUILD_DURATION_HISTOGRAM, MetricsCategory.PROJECT, project, tags);
    }

    abstract static class GaugeWrapper implements Gauge<Long> {

        public abstract Long getResult();

        @Override
        public Long getValue() {
            try {
                return getResult();
            } catch (Exception e) {
                log.error("Exception happens.", e);
            }
            return 0L;
        }
    }

}

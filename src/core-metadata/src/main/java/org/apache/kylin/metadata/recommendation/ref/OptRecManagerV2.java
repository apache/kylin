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

package org.apache.kylin.metadata.recommendation.ref;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.optimization.FrequencyMap;
import org.apache.kylin.metadata.cube.optimization.GarbageLayoutType;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.recommendation.candidate.LayoutMetric;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.candidate.RawRecManager;
import org.apache.kylin.metadata.recommendation.entity.LayoutRecItemV2;
import org.apache.kylin.metadata.recommendation.util.RawRecUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OptRecManagerV2 {

    public static OptRecManagerV2 getInstance(String project) {
        return Singletons.getInstance(project, OptRecManagerV2.class);
    }

    private final String project;

    OptRecManagerV2(String project) {
        this.project = project;
    }

    public OptRecV2 loadOptRecV2(String uuid) {
        Preconditions.checkState(StringUtils.isNotEmpty(uuid));
        OptRecV2 optRecV2 = new OptRecV2(project, uuid, true);
        optRecV2.initRecommendation();
        List<Integer> brokenLayoutIds = Lists.newArrayList(optRecV2.getBrokenRefIds());
        if (!brokenLayoutIds.isEmpty()) {
            log.debug("recognized broken index ids: {}", brokenLayoutIds);
            RawRecManager.getInstance(project).removeByIds(brokenLayoutIds);
        }
        return optRecV2;
    }

    public void discardAll(String uuid) {
        OptRecV2 optRecV2 = loadOptRecV2(uuid);
        List<Integer> rawIds = optRecV2.getRawIds();
        Map<Integer, RawRecItem> rawRecItemMap = optRecV2.getRawRecItemMap();
        List<Integer> layoutRawIds = Lists.newArrayList();
        rawIds.forEach(recId -> {
            if (rawRecItemMap.get(recId).isLayoutRec()) {
                layoutRawIds.add(recId);
            }
        });
        RawRecManager rawManager = RawRecManager.getInstance(project);
        rawManager.discardByIds(layoutRawIds);
    }

    public boolean genRecItemsFromIndexOptimizer(String project, String modelId,
            Map<Long, GarbageLayoutType> garbageLayouts) {
        if (garbageLayouts.isEmpty()) {
            return false;
        }
        log.info("Generating raw recommendations from index optimizer for model({}/{})", project, modelId);
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataModelManager modelManager = NDataModelManager.getInstance(config, project);
        NDataModel model = modelManager.getDataModelDesc(modelId);

        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(config, project);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelId);
        Map<Long, LayoutEntity> allLayoutsMap = indexPlan.getAllLayoutsMap();

        NDataflowManager dataflowManager = NDataflowManager.getInstance(config, project);
        NDataflow dataflow = dataflowManager.getDataflow(modelId);
        Map<Long, FrequencyMap> hitFrequencyMap = dataflow.getLayoutHitCount();

        RawRecManager recManager = RawRecManager.getInstance(project);
        Map<String, RawRecItem> layoutRecommendations = recManager.queryNonAppliedLayoutRawRecItems(modelId, false);
        Map<String, List<String>> md5ToUuid = RawRecUtil.uniqueFlagsToMd5Map(layoutRecommendations.keySet());
        AtomicInteger newRecCount = new AtomicInteger(0);
        RawRecManager rawRecManager = RawRecManager.getInstance(project);
        garbageLayouts.forEach((layoutId, type) -> {
            LayoutEntity layout = allLayoutsMap.get(layoutId);
            String content = RawRecUtil.getContent(project, modelId, layout.genUniqueContent(),
                    RawRecItem.RawRecType.REMOVAL_LAYOUT);
            String md5 = RawRecUtil.computeMD5(content);

            AtomicBoolean retry = new AtomicBoolean(false);
            JdbcUtil.withTxAndRetry(rawRecManager.getTransactionManager(), () -> {
                Pair<String, RawRecItem> recItemPair = null;
                if (retry.get()) {
                    recItemPair = recManager.queryRecItemByMd5(md5, content);
                } else {
                    retry.set(true);
                    recItemPair = RawRecUtil.getRawRecItemFromMap(md5, content, md5ToUuid, layoutRecommendations);
                }
                FrequencyMap frequencyMap = hitFrequencyMap.getOrDefault(layoutId, new FrequencyMap());
                RawRecItem recItem;
                if (recItemPair.getSecond() != null) {
                    recItem = recItemPair.getSecond();
                    recItem.setUpdateTime(System.currentTimeMillis());
                    recItem.setRecSource(type.name());
                    if (recItem.getState() == RawRecItem.RawRecState.DISCARD) {
                        recItem.setState(RawRecItem.RawRecState.INITIAL);
                        LayoutMetric layoutMetric = recItem.getLayoutMetric();
                        if (layoutMetric == null) {
                            recItem.setLayoutMetric(new LayoutMetric(frequencyMap, new LayoutMetric.LatencyMap()));
                        } else {
                            layoutMetric.setFrequencyMap(frequencyMap);
                        }
                    }
                } else {
                    LayoutRecItemV2 item = new LayoutRecItemV2();
                    item.setLayout(layout);
                    item.setCreateTime(System.currentTimeMillis());
                    item.setAgg(layout.getId() < IndexEntity.TABLE_INDEX_START_ID);
                    item.setUuid(RandomUtil.randomUUIDStr());

                    recItem = new RawRecItem(project, modelId, model.getSemanticVersion(),
                            RawRecItem.RawRecType.REMOVAL_LAYOUT);
                    recItem.setRecEntity(item);
                    recItem.setCreateTime(item.getCreateTime());
                    recItem.setUpdateTime(item.getCreateTime());
                    recItem.setState(RawRecItem.RawRecState.INITIAL);
                    recItem.setUniqueFlag(recItemPair.getFirst());
                    recItem.setDependIDs(item.genDependIds());
                    recItem.setLayoutMetric(new LayoutMetric(frequencyMap, new LayoutMetric.LatencyMap()));
                    recItem.setRecSource(type.name());
                    newRecCount.getAndIncrement();
                }
                if (recItem.getLayoutMetric() != null) {
                    RawRecManager.getInstance(project).saveOrUpdate(recItem);
                }
                return null;
            });
        });
        log.info("Raw recommendations from index optimizer for model({}/{}) successfully generated.", project, modelId);
        return newRecCount.get() > 0;
    }
}

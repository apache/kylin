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

package org.apache.kylin.metadata.recommendation.candidate;

import static org.apache.kylin.metadata.favorite.FavoriteRule.MIN_HIT_COUNT;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem.CostMethod;
import org.apache.kylin.metadata.recommendation.ref.LayoutRef;
import org.apache.kylin.metadata.recommendation.ref.OptRecV2;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RawRecManager {

    private final String project;
    private final JdbcRawRecStore jdbcRawRecStore;

    // CONSTRUCTOR
    public static RawRecManager getInstance(String project) {
        return Singletons.getInstance(project, RawRecManager.class);
    }

    RawRecManager(String project) throws Exception {
        this.project = project;
        this.jdbcRawRecStore = new JdbcRawRecStore(KylinConfig.getInstanceFromEnv());
    }

    /**
     * Load CC, Dimension, measure RawRecItems of given models.
     * If models not given, load these RawRecItems of the whole project.
     */
    public Map<String, RawRecItem> queryNonLayoutRecItems(Set<String> modelIdSet) {
        if (CollectionUtils.isNotEmpty(modelIdSet) && modelIdSet.size() == 1) {
            return queryNonLayoutRecItems(modelIdSet.iterator().next());
        }

        Map<String, NDataModel> allModelMap = Maps.newHashMap();
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.readSystemKylinConfig(), project);
        List<NDataModel> models = modelManager.listAllModels();
        models.removeIf(NDataModel::isBroken);
        models.forEach(model -> allModelMap.put(model.getUuid(), model));
        if (CollectionUtils.isNotEmpty(modelIdSet)) {
            allModelMap.entrySet().removeIf(entry -> modelIdSet.contains(entry.getKey()));
        }

        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryNonLayoutRecItems(project);
        Map<String, RawRecItem> allRecItems = Maps.newHashMap();
        rawRecItems.forEach(recItem -> {
            NDataModel model = allModelMap.get(recItem.getModelID());
            if (model != null && !recItem.isOutOfDate(model.getSemanticVersion())) {
                allRecItems.put(recItem.getUniqueFlag(), recItem);
            }
        });
        return allRecItems;
    }

    private Map<String, RawRecItem> queryNonLayoutRecItems(String model) {
        Map<String, RawRecItem> recItemMap = Maps.newHashMap();
        List<RawRecItem> recItems = jdbcRawRecStore.queryNonLayoutRecItems(project, model);
        if (CollectionUtils.isEmpty(recItems)) {
            log.info("There is no raw recommendations of model({}/{}})", project, model);
            return recItemMap;
        }
        recItems.forEach(recItem -> recItemMap.putIfAbsent(recItem.getUniqueFlag(), recItem));
        return recItemMap;
    }

    public Map<String, RawRecItem> queryNonAppliedLayoutRawRecItems(String model, boolean isAdditionalRec) {
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryNonAppliedLayoutRecItems(project, model, isAdditionalRec);
        Map<String, RawRecItem> map = Maps.newHashMap();
        rawRecItems.forEach(recItem -> map.put(recItem.getUniqueFlag(), recItem));
        return map;
    }

    public int clearExistingCandidates(String project, String model) {
        int updateCount = 0;
        long start = System.currentTimeMillis();
        List<RawRecItem> existingCandidates = jdbcRawRecStore.queryAdditionalLayoutRecItems(project, model);
        long updateTime = System.currentTimeMillis();
        for (val rawRecItem : existingCandidates) {
            rawRecItem.setUpdateTime(updateTime);
            if (!RawRecItem.IMPORTED.equalsIgnoreCase(rawRecItem.getRecSource())) {
                rawRecItem.setState(RawRecItem.RawRecState.INITIAL);
                updateCount++;
            }
        }
        jdbcRawRecStore.update(existingCandidates);
        log.info("clear all existing candidate recommendations of model({}/{}) takes {} ms.", //
                project, model, System.currentTimeMillis() - start);
        return updateCount;
    }

    public List<RawRecItem> displayTopNRecItems(String project, String model, int limit) {
        return jdbcRawRecStore.chooseTopNCandidates(project, model, limit, 0, RawRecItem.RawRecState.RECOMMENDED);
    }

    public List<RawRecItem> queryImportedRawRecItems(String project, String model) {
        return jdbcRawRecStore.queryImportedRawRecItems(project, model, RawRecItem.RawRecState.RECOMMENDED);
    }

    public boolean updateRecommendedTopN(String project, String model, int topN) {
        long current = System.currentTimeMillis();
        RawRecManager rawRecManager = RawRecManager.getInstance(project);
        int existCandidateCount = rawRecManager.clearExistingCandidates(project, model);
        OptRecV2 optRecV2 = new OptRecV2(project, model, false);
        List<RawRecItem> topNCandidates = Lists.newArrayList();
        int minCost = Integer.parseInt(
                FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getValue(MIN_HIT_COUNT));
        CostMethod costMethod = CostMethod.getCostMethod(project);
        minCost = costMethod == CostMethod.HIT_COUNT ? minCost : -1;
        int offset = 0;
        while (topNCandidates.size() < topN) {
            List<RawRecItem> rawRecItems = jdbcRawRecStore.chooseTopNCandidates(project, model, minCost, topN, offset,
                    RawRecItem.RawRecState.INITIAL);
            if (CollectionUtils.isEmpty(rawRecItems)) {
                break;
            }
            optRecV2.filterExcludedRecPatterns(rawRecItems);
            rawRecItems.forEach(recItem -> {
                LayoutRef layoutRef = optRecV2.getAdditionalLayoutRefs().get(-recItem.getId());
                if (layoutRef.isExcluded()) {
                    return;
                }
                topNCandidates.add(optRecV2.getRawRecItemMap().get(recItem.getId()));
            });
            offset++;
        }
        topNCandidates.forEach(rawRecItem -> {
            rawRecItem.setUpdateTime(current);
            rawRecItem.setRecSource(RawRecItem.QUERY_HISTORY);
            rawRecItem.setState(RawRecItem.RawRecState.RECOMMENDED);
        });
        rawRecManager.saveOrUpdate(topNCandidates);
        return topNCandidates.size() != existCandidateCount;
    }

    public Map<RawRecItem.RawRecType, Integer> getCandidatesByProject(String project) {
        RawRecItem.RawRecType[] rawRecTypes = { RawRecItem.RawRecType.ADDITIONAL_LAYOUT,
                RawRecItem.RawRecType.REMOVAL_LAYOUT };
        int additionalCandidateCount = jdbcRawRecStore.getRecItemCountByProject(project, rawRecTypes[0]);
        int removalCandidatesCount = jdbcRawRecStore.getRecItemCountByProject(project, rawRecTypes[1]);
        Map<RawRecItem.RawRecType, Integer> map = Maps.newHashMap();
        map.put(RawRecItem.RawRecType.ADDITIONAL_LAYOUT, additionalCandidateCount);
        map.put(RawRecItem.RawRecType.REMOVAL_LAYOUT, removalCandidatesCount);
        return map;
    }

    public List<RawRecItem> getCandidatesByProjectAndBenefit(String project, int limit) {
        throw new NotImplementedException("get candidate raw recommendations by project not implement!");
    }

    public void saveOrUpdate(List<RawRecItem> recItems) {
        jdbcRawRecStore.batchAddOrUpdate(recItems);
    }

    public void discardRecItemsOfBrokenModel(String model) {
        jdbcRawRecStore.discardRecItemsOfBrokenModel(model);
    }

    public void deleteByProject(String project) {
        jdbcRawRecStore.deleteByProject(project);
    }

    public void cleanForDeletedProject(List<String> projectList) {
        jdbcRawRecStore.cleanForDeletedProject(projectList);
    }

    public void removeByIds(List<Integer> idList) {
        jdbcRawRecStore.updateState(idList, RawRecItem.RawRecState.BROKEN);
    }

    public void applyByIds(List<Integer> idList) {
        jdbcRawRecStore.updateState(idList, RawRecItem.RawRecState.APPLIED);
    }

    public void discardByIds(List<Integer> idList) {
        jdbcRawRecStore.updateState(idList, RawRecItem.RawRecState.DISCARD);
    }

    public Set<String> updateAllCost(String project) {
        return jdbcRawRecStore.updateAllCost(project);
    }

    public int getMaxId() {
        return jdbcRawRecStore.getMaxId();
    }

    public int getMinId() {
        return jdbcRawRecStore.getMinId();
    }

    public RawRecItem getRawRecItemByUniqueFlag(String project, String modelId, String uniqueFlag,
            Integer semanticVersion) {
        return jdbcRawRecStore.queryByUniqueFlag(project, modelId, uniqueFlag, semanticVersion);
    }

    public void importRecommendations(String project, String targetModelId, List<RawRecItem> recItems) {
        jdbcRawRecStore.importRecommendations(project, targetModelId, recItems);
    }
}

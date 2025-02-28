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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.ArrayListMultimap;
import org.apache.kylin.guava30.shaded.common.collect.ListMultimap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.optimization.FrequencyMap;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryHistorySql;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.metadata.recommendation.candidate.LayoutMetric;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.candidate.RawRecManager;
import org.apache.kylin.metadata.recommendation.ref.OptRecManagerV2;
import org.apache.kylin.metadata.recommendation.ref.OptRecV2;
import org.apache.kylin.metadata.recommendation.util.RawRecUtil;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.ModelReuseContext;
import org.apache.kylin.rec.ProposerJob;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.util.QueryRecStatsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Component("rawRecService")
@Primary
public class RawRecService extends BasicService
        implements ModelChangeSupporter, ProjectSmartSupporter, QuerySmartSupporter {
    private static final Logger log = LoggerFactory.getLogger("smart");

    @Autowired
    ProjectSmartService projectSmartService;

    @Autowired
    OptRecService optRecService;

    public static int recommendationSize(String project) {
        FavoriteRuleManager ruleManager = FavoriteRuleManager.getInstance(project);
        FavoriteRule favoriteRule = ruleManager.getOrDefaultByName(FavoriteRule.REC_SELECT_RULE_NAME);
        FavoriteRule.Condition condition = (FavoriteRule.Condition) favoriteRule.getConds().get(0);
        return Integer.parseInt(condition.getRightThreshold());
    }

    public void accelerate(String project) {
        projectSmartService.accelerateImmediately(project);
        updateCostsAndTopNCandidates(project);
    }

    public void transferAndSaveRecommendations(AbstractContext proposeContext) {
        if (!(proposeContext instanceof ModelReuseContext)) {
            return;
        }
        //filter modelcontext(by create model)
        proposeContext.setModelContexts(proposeContext.getModelContexts().stream()
                .filter(modelContext -> modelContext.getOriginModel() != null).collect(Collectors.toList()));

        ModelReuseContext semiContextV2 = (ModelReuseContext) proposeContext;
        Map<String, RawRecItem> nonLayoutUniqueFlagRecMap = transferAndSaveModelRelatedRecItems(semiContextV2);

        transferToLayoutRecItemsAndSave(semiContextV2, ArrayListMultimap.create(), nonLayoutUniqueFlagRecMap);
    }

    public void generateRawRecommendations(String project, List<QueryHistory> queryHistories) {
        if (CollectionUtils.isEmpty(queryHistories)) {
            return;
        }

        long startTime = System.currentTimeMillis();
        log.info("Semi-Auto-Mode project:{} generate suggestions by sqlList size: {}", project, queryHistories.size());
        List<String> sqlList = Lists.newArrayList();
        ListMultimap<String, QueryHistory> queryHistoryMap = ArrayListMultimap.create();
        queryHistories.forEach(queryHistory -> {
            QueryHistorySql queryHistorySql = queryHistory.getQueryHistorySql();
            String normalizedSql = queryHistorySql.getNormalizedSql();
            sqlList.add(normalizedSql);
            queryHistoryMap.put(normalizedSql, queryHistory);
        });

        KylinConfig projectConfig = NProjectManager.getProjectConfig(project);
        AbstractContext semiContextV2 = ProposerJob
                .propose(new ModelReuseContext(projectConfig, project, sqlList.toArray(new String[0])));

        Map<String, RawRecItem> nonLayoutRecItemMap = transferAndSaveModelRelatedRecItems(semiContextV2);

        ArrayListMultimap<String, QueryHistory> layoutToQHMap = ArrayListMultimap.create();
        for (AccelerateInfo accelerateInfo : semiContextV2.getAccelerateInfoMap().values()) {
            for (AccelerateInfo.QueryLayoutRelation layout : accelerateInfo.getRelatedLayouts()) {
                List<QueryHistory> queryHistoryList = queryHistoryMap.get(layout.getSql());
                layoutToQHMap.putAll(layout.getModelId() + "_" + layout.getLayoutId(), queryHistoryList);
            }
        }

        transferToLayoutRecItemsAndSave(semiContextV2, layoutToQHMap, nonLayoutRecItemMap);

        markFailAccelerateMessageToQueryHistory(queryHistoryMap, semiContextV2);

        // Do v3 recommendation and query statistics.
        QueryRecStatsCollector.getInstance().doStatistics(queryHistoryMap, nonLayoutRecItemMap, semiContextV2);

        log.info("Semi-Auto-Mode project:{} generate suggestions cost {}ms", project,
                System.currentTimeMillis() - startTime);
    }

    private Map<String, RawRecItem> transferAndSaveModelRelatedRecItems(AbstractContext semiContext) {
        Map<String, RawRecItem> nonLayoutUniqueFlagRecMap = RawRecManager.getInstance(semiContext.getProject())
                .queryNonLayoutRecItems(null);
        List<RawRecItem> ccRawRecItems = transferToCCRawRecItemAndSave(semiContext, nonLayoutUniqueFlagRecMap);
        ccRawRecItems.forEach(recItem -> nonLayoutUniqueFlagRecMap.put(recItem.getUniqueFlag(), recItem));

        List<RawRecItem> dimensionRecItems = transferToDimensionRecItemsAndSave(semiContext, nonLayoutUniqueFlagRecMap);
        List<RawRecItem> measureRecItems = transferToMeasureRecItemsAndSave(semiContext, nonLayoutUniqueFlagRecMap);
        dimensionRecItems.forEach(recItem -> nonLayoutUniqueFlagRecMap.put(recItem.getUniqueFlag(), recItem));
        measureRecItems.forEach(recItem -> nonLayoutUniqueFlagRecMap.put(recItem.getUniqueFlag(), recItem));

        return nonLayoutUniqueFlagRecMap;
    }

    public void markFailAccelerateMessageToQueryHistory(ListMultimap<String, QueryHistory> queryHistoryMap,
            AbstractContext semiContextV2) {
        List<Pair<Long, QueryHistoryInfo>> idToQHInfoList = Lists.newArrayList();
        semiContextV2.getAccelerateInfoMap().forEach((sql, accelerateInfo) -> {
            if (!accelerateInfo.isNotSucceed()) {
                return;
            }
            queryHistoryMap.get(sql).forEach(qh -> {
                QueryHistoryInfo queryHistoryInfo = qh.getQueryHistoryInfo();
                if (queryHistoryInfo == null) {
                    queryHistoryInfo = new QueryHistoryInfo();
                }
                if (accelerateInfo.isFailed()) {
                    Throwable cause = accelerateInfo.getFailedCause();
                    String failMessage = cause == null ? null : cause.getMessage();
                    if (failMessage != null && failMessage.length() > 256) {
                        failMessage = failMessage.substring(0, 256);
                    }
                    queryHistoryInfo.setErrorMsg(failMessage);
                } else if (accelerateInfo.isPending()) {
                    queryHistoryInfo.setErrorMsg(accelerateInfo.getPendingMsg());
                }
                idToQHInfoList.add(new Pair<>(qh.getId(), queryHistoryInfo));
            });
        });
        RDBMSQueryHistoryDAO.getInstance().batchUpdateQueryHistoriesInfo(idToQHInfoList);
    }

    public void updateCostsAndTopNCandidates(String projectName) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        List<ProjectInstance> projectList = Lists.newArrayList();
        if (StringUtils.isEmpty(projectName)) {
            List<ProjectInstance> instances = getManager(NProjectManager.class).listAllProjects().stream() //
                    .filter(projectInstance -> !projectInstance.isExpertMode()) //
                    .collect(Collectors.toList());
            projectList.addAll(instances);
        } else {
            ProjectInstance instance = getManager(NProjectManager.class).getProject(projectName);
            projectList.add(instance);
        }

        for (ProjectInstance projectInstance : projectList) {
            String project = projectInstance.getName();
            if (projectInstance.isExpertMode()) {
                continue;
            }
            try {
                log.info("Running update cost for project<{}>", project);
                RawRecManager rawRecManager = RawRecManager.getInstance(project);
                NDataModelManager modelManager = NDataModelManager.getInstance(kylinConfig, project);
                Set<String> models = rawRecManager.updateAllCost(project);
                int topN = recommendationSize(project);

                Set<String> needUpdateModels = Sets.newHashSet();
                for (String model : models) {
                    long current = System.currentTimeMillis();
                    NDataModel dataModel = modelManager.getDataModelDesc(model);
                    if (dataModel == null || dataModel.isBroken()) {
                        log.warn("Broken(or nonExist) model({}/{}) cannot update recommendations.", project, model);
                        continue;
                    }

                    log.info("Running update topN raw recommendation for model({}/{}).", project, model);
                    boolean recommendationCountChange = rawRecManager.updateRecommendedTopN(project, model, topN);
                    if (recommendationCountChange) {
                        needUpdateModels.add(model);
                    }
                    log.info("Update topN raw recommendations for model({}/{}) takes {} ms", //
                            project, model, System.currentTimeMillis() - current);
                }
                optRecService.updateRecommendationCount(project, needUpdateModels);
            } catch (Exception e) {
                log.error("Update cost and update topN failed for project({})", project, e);
            }
        }
    }

    @Override
    public void onUpdate(String project, String modelId) {
        ProjectInstance prjInstance = getManager(NProjectManager.class).getProject(project);
        if (prjInstance.isSemiAutoMode()) {
            OptRecManagerV2.getInstance(project).loadOptRecV2(modelId);
            optRecService.updateRecommendationCount(project, modelId);
        }
    }

    @Override
    public void onUpdateSingle(String project, String modelId) {
        optRecService.updateRecommendationCount(project, modelId);
    }

    @Override
    public int getGaugeSize(String project, String modelId) {
        return optRecService.getOptRecLayoutsResponse(project, modelId, OptRecService.RecActionType.REMOVE_INDEX.name())
                .getSize();
    }

    @Override
    public int getRecItemSize(String project, String modelId) {
        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(project).loadOptRecV2(modelId);
        List<RawRecItem> rawRecItems = optRecService.getRecLayout(optRecV2, OptRecService.RecActionType.ALL);
        return CollectionUtils.isEmpty(rawRecItems) ? 0 : rawRecItems.size();
    }

    @Override
    public void onUpdateCost(String project) {
        updateCostsAndTopNCandidates(project);
    }

    @Override
    public int onShowSize(String project) {
        return recommendationSize(project);
    }

    @Override
    public void onMatchQueryHistory(String project, List<QueryHistory> queries) {
        generateRawRecommendations(project, queries);
    }

    void transferToLayoutRecItemsAndSave(AbstractContext semiContextV2,
            ArrayListMultimap<String, QueryHistory> layoutToQHMap, Map<String, RawRecItem> nonLayoutUniqueFlagRecMap) {
        RawRecManager recManager = RawRecManager.getInstance(semiContextV2.getProject());
        String recSource = layoutToQHMap.isEmpty() ? RawRecItem.IMPORTED : RawRecItem.QUERY_HISTORY;
        for (AbstractContext.ModelContext modelContext : semiContextV2.getModelContexts()) {
            NDataModel targetModel = modelContext.getTargetModel();
            if (targetModel == null) {
                continue;
            }

            Map<String, RawRecItem> layoutUniqueFlagRecMap = recManager
                    .queryNonAppliedLayoutRawRecItems(targetModel.getUuid(), true);
            Map<String, List<String>> md5ToFlags = RawRecUtil.uniqueFlagsToMd5Map(layoutUniqueFlagRecMap.keySet());
            Set<String> newCcUuids = modelContext.getCcRecItemMap().values().stream()
                    .map(item -> item.getCc().getUuid()).collect(Collectors.toSet());

            modelContext.getIndexRexItemMap().forEach((itemUUID, layoutItem) -> {
                // update layout content first
                layoutItem.updateLayoutContent(targetModel, nonLayoutUniqueFlagRecMap, newCcUuids);
                String content = RawRecUtil.getContent(semiContextV2.getProject(), targetModel.getUuid(),
                        layoutItem.getUniqueContent(), RawRecItem.RawRecType.ADDITIONAL_LAYOUT);
                String md5 = RawRecUtil.computeMD5(content);

                AtomicBoolean retry = new AtomicBoolean(false);
                JdbcUtil.withTxAndRetry(recManager.getTransactionManager(), () -> {
                    Pair<String, RawRecItem> recItemPair;
                    if (retry.get()) {
                        recItemPair = recManager.queryRecItemByMd5(md5, content);
                    } else {
                        retry.set(true);
                        recItemPair = RawRecUtil.getRawRecItemFromMap(md5, content, md5ToFlags, layoutUniqueFlagRecMap);
                    }
                    RawRecItem recItem;
                    if (recItemPair.getSecond() != null) {
                        recItem = recItemPair.getSecond();
                        recItem.setUpdateTime(System.currentTimeMillis());
                        recItem.restoreIfNeed();
                    } else {
                        recItem = new RawRecItem(semiContextV2.getProject(), //
                                targetModel.getUuid(), //
                                targetModel.getSemanticVersion(), //
                                RawRecItem.RawRecType.ADDITIONAL_LAYOUT);
                        recItem.setRecEntity(layoutItem);
                        recItem.setCreateTime(layoutItem.getCreateTime());
                        recItem.setUpdateTime(layoutItem.getCreateTime());
                        recItem.setState(RawRecItem.RawRecState.INITIAL);
                        recItem.setUniqueFlag(recItemPair.getFirst());
                    }
                    recItem.setDependIDs(layoutItem.genDependIds());
                    recItem.setRecSource(recSource);
                    if (recSource.equalsIgnoreCase(RawRecItem.IMPORTED)) {
                        recItem.cleanLayoutStatistics();
                        recItem.setState(RawRecItem.RawRecState.RECOMMENDED);
                    }
                    updateLayoutStatistic(recItem, layoutToQHMap, layoutItem.getLayout(), targetModel);
                    if (recItem.isAdditionalRecItemSavable()) {
                        recManager.saveOrUpdate(recItem);
                        nonLayoutUniqueFlagRecMap.put(layoutItem.getUniqueId(targetModel.getUuid()), recItem);
                    }
                    return null;
                });
            });
        }
    }

    private void updateLayoutStatistic(RawRecItem recItem, ListMultimap<String, QueryHistory> layoutToQHMap,
            LayoutEntity layout, NDataModel targetModel) {
        if (layoutToQHMap.isEmpty()) {
            return;
        }
        List<QueryHistory> queryHistories = layoutToQHMap.get(targetModel.getId() + "_" + layout.getId());
        if (CollectionUtils.isEmpty(queryHistories)) {
            return;
        }
        LayoutMetric layoutMetric = recItem.getLayoutMetric();
        if (layoutMetric == null) {
            layoutMetric = new LayoutMetric(new FrequencyMap(), new LayoutMetric.LatencyMap());
        }

        LayoutMetric.LatencyMap latencyMap = layoutMetric.getLatencyMap();
        FrequencyMap frequencyMap = layoutMetric.getFrequencyMap();
        double totalTime = recItem.getTotalTime();
        double maxTime = recItem.getMaxTime();
        long minTime = Long.MAX_VALUE;
        int hitCount = recItem.getHitCount();
        for (QueryHistory qh : queryHistories) {
            hitCount++;
            long duration = qh.getDuration();
            totalTime = totalTime + duration;
            if (duration > maxTime) {
                maxTime = duration;
            }
            if (duration < minTime) {
                minTime = duration;
            }

            latencyMap.incLatency(qh.getQueryTime(), duration);
            frequencyMap.incFrequency(qh.getQueryTime());
        }
        recItem.setTotalTime(totalTime);
        recItem.setMaxTime(maxTime);
        recItem.setMinTime(minTime);

        recItem.setLayoutMetric(layoutMetric);
        recItem.setHitCount(hitCount);
    }

    private List<RawRecItem> transferToMeasureRecItemsAndSave(AbstractContext semiContextV2,
            Map<String, RawRecItem> nonLayoutUniqueFlagRecMap) {
        RawRecManager recManager = RawRecManager.getInstance(semiContextV2.getProject());
        ArrayList<RawRecItem> rawRecItems = Lists.newArrayList();
        Map<String, List<String>> md5ToFlags = RawRecUtil.uniqueFlagsToMd5Map(nonLayoutUniqueFlagRecMap.keySet());
        Map<String, RawRecItem> uuidToRecItemMap = new HashMap<>();
        nonLayoutUniqueFlagRecMap.values().forEach(recItem -> {
            uuidToRecItemMap.put(recItem.getRecEntity().getUuid(), recItem);
        });
        for (AbstractContext.ModelContext modelContext : semiContextV2.getModelContexts()) {
            if (modelContext.getTargetModel() == null) {
                continue;
            }
            modelContext.getMeasureRecItemMap().forEach((uniqueFlag, measureItem) -> {
                String content = RawRecUtil.getContent(semiContextV2.getProject(),
                        modelContext.getTargetModel().getUuid(), measureItem.getUniqueContent(),
                        RawRecItem.RawRecType.MEASURE);
                String md5 = RawRecUtil.computeMD5(content);

                AtomicBoolean retry = new AtomicBoolean(false);
                JdbcUtil.withTxAndRetry(recManager.getTransactionManager(), () -> {
                    Pair<String, RawRecItem> recItemPair = null;
                    if (retry.get()) {
                        recItemPair = recManager.queryRecItemByMd5(md5, content);
                    } else {
                        retry.set(true);
                        recItemPair = RawRecUtil.getRawRecItemFromMap(md5, content, md5ToFlags,
                                nonLayoutUniqueFlagRecMap);
                    }
                    RawRecItem item;
                    if (recItemPair.getSecond() != null) {
                        item = recItemPair.getSecond();
                        item.setUpdateTime(System.currentTimeMillis());
                    } else {
                        item = new RawRecItem(semiContextV2.getProject(), //
                                modelContext.getTargetModel().getUuid(), //
                                modelContext.getTargetModel().getSemanticVersion(), //
                                RawRecItem.RawRecType.MEASURE);
                        item.setUniqueFlag(recItemPair.getFirst());
                        item.setState(RawRecItem.RawRecState.INITIAL);
                        item.setCreateTime(measureItem.getCreateTime());
                        item.setUpdateTime(measureItem.getCreateTime());
                        item.setRecEntity(measureItem);
                    }
                    item.setDependIDs(measureItem.genDependIds(uuidToRecItemMap, measureItem.getUniqueContent(),
                            getOriginModel(semiContextV2.getProject(), modelContext)));
                    recManager.saveOrUpdate(item);
                    rawRecItems.add(item);
                    return null;
                });
            });
        }
        return rawRecItems;
    }

    private List<RawRecItem> transferToDimensionRecItemsAndSave(AbstractContext semiContextV2,
            Map<String, RawRecItem> uniqueRecItemMap) {
        RawRecManager recManager = RawRecManager.getInstance(semiContextV2.getProject());
        ArrayList<RawRecItem> rawRecItems = Lists.newArrayList();
        Map<String, List<String>> md5ToFlags = RawRecUtil.uniqueFlagsToMd5Map(uniqueRecItemMap.keySet());
        Map<String, RawRecItem> uuidToRecItemMap = new HashMap<>();
        uniqueRecItemMap.values().forEach(recItem -> {
            uuidToRecItemMap.put(recItem.getRecEntity().getUuid(), recItem);
        });
        for (AbstractContext.ModelContext modelContext : semiContextV2.getModelContexts()) {
            if (modelContext.getTargetModel() == null) {
                continue;
            }
            modelContext.getDimensionRecItemMap().forEach((uniqueFlag, dimItem) -> {
                String content = RawRecUtil.getContent(semiContextV2.getProject(),
                        modelContext.getTargetModel().getUuid(), dimItem.getUniqueContent(),
                        RawRecItem.RawRecType.DIMENSION);
                String md5 = RawRecUtil.computeMD5(content);

                AtomicBoolean retry = new AtomicBoolean(false);
                JdbcUtil.withTxAndRetry(recManager.getTransactionManager(), () -> {
                    Pair<String, RawRecItem> recItemPair = null;
                    if (retry.get()) {
                        recItemPair = recManager.queryRecItemByMd5(md5, content);
                    } else {
                        retry.set(true);
                        recItemPair = RawRecUtil.getRawRecItemFromMap(md5, content, md5ToFlags, uniqueRecItemMap);
                    }
                    RawRecItem item;
                    if (recItemPair.getSecond() != null) {
                        item = recItemPair.getSecond();
                        item.setUpdateTime(System.currentTimeMillis());
                    } else {
                        item = new RawRecItem(semiContextV2.getProject(), //
                                modelContext.getTargetModel().getUuid(), //
                                modelContext.getTargetModel().getSemanticVersion(), //
                                RawRecItem.RawRecType.DIMENSION);
                        item.setUniqueFlag(recItemPair.getFirst());
                        item.setCreateTime(dimItem.getCreateTime());
                        item.setUpdateTime(dimItem.getCreateTime());
                        item.setState(RawRecItem.RawRecState.INITIAL);
                        item.setRecEntity(dimItem);
                    }
                    item.setDependIDs(dimItem.genDependIds(uuidToRecItemMap, dimItem.getUniqueContent(),
                            getOriginModel(semiContextV2.getProject(), modelContext)));
                    recManager.saveOrUpdate(item);
                    rawRecItems.add(item);
                    return null;
                });
            });
        }
        return rawRecItems;
    }

    private List<RawRecItem> transferToCCRawRecItemAndSave(AbstractContext semiContextV2,
            Map<String, RawRecItem> uniqueRecItemMap) {
        RawRecManager recManager = RawRecManager.getInstance(semiContextV2.getProject());
        List<RawRecItem> rawRecItems = Lists.newArrayList();
        Map<String, List<String>> md5ToFlags = RawRecUtil.uniqueFlagsToMd5Map(uniqueRecItemMap.keySet());
        for (AbstractContext.ModelContext modelContext : semiContextV2.getModelContexts()) {
            if (modelContext.getTargetModel() == null) {
                continue;
            }

            modelContext.getCcRecItemMap().forEach((uniqueFlag, ccItem) -> {
                String content = RawRecUtil.getContent(semiContextV2.getProject(),
                        modelContext.getTargetModel().getUuid(), ccItem.getUniqueContent(),
                        RawRecItem.RawRecType.COMPUTED_COLUMN);
                String md5 = RawRecUtil.computeMD5(content);

                AtomicBoolean retry = new AtomicBoolean(false);
                JdbcUtil.withTxAndRetry(recManager.getTransactionManager(), () -> {
                    Pair<String, RawRecItem> recItemPair = null;
                    if (retry.get()) {
                        recItemPair = recManager.queryRecItemByMd5(md5, content);
                    } else {
                        retry.set(true);
                        recItemPair = RawRecUtil.getRawRecItemFromMap(md5, content, md5ToFlags, uniqueRecItemMap);
                    }
                    RawRecItem item;
                    if (recItemPair.getSecond() != null) {
                        item = recItemPair.getSecond();
                        item.setUpdateTime(System.currentTimeMillis());
                    } else {
                        item = new RawRecItem(semiContextV2.getProject(), //
                                modelContext.getTargetModel().getUuid(), //
                                modelContext.getTargetModel().getSemanticVersion(), //
                                RawRecItem.RawRecType.COMPUTED_COLUMN);
                        item.setCreateTime(ccItem.getCreateTime());
                        item.setUpdateTime(ccItem.getCreateTime());
                        item.setUniqueFlag(recItemPair.getFirst());
                        item.setRecEntity(ccItem);
                        item.setState(RawRecItem.RawRecState.INITIAL);
                    }
                    item.setDependIDs(ccItem.genDependIds(getOriginModel(semiContextV2.getProject(), modelContext)));
                    recManager.saveOrUpdate(item);
                    rawRecItems.add(item);
                    return null;
                });
            });
        }
        return rawRecItems;
    }

    private NDataModel getOriginModel(String project, AbstractContext.ModelContext modelContext) {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        return modelContext.getOriginModel().getJoinTables().size() == modelContext.getTargetModel().getJoinTables()
                .size() ? modelContext.getOriginModel()
                        : modelManager.getDataModelDesc(modelContext.getOriginModel().getUuid());
    }
}

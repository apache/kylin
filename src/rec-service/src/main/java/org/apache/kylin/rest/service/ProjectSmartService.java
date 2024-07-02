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

import static org.apache.kylin.metadata.favorite.FavoriteRule.AUTO_INDEX_PLAN_RULE_NAMES;
import static org.apache.kylin.metadata.favorite.FavoriteRule.EFFECTIVE_DAYS;
import static org.apache.kylin.metadata.favorite.FavoriteRule.FAVORITE_RULE_NAMES;
import static org.apache.kylin.metadata.favorite.FavoriteRule.FREQUENCY_TIME_WINDOW;
import static org.apache.kylin.metadata.favorite.FavoriteRule.LOW_FREQUENCY_THRESHOLD;
import static org.apache.kylin.metadata.favorite.FavoriteRule.MIN_HIT_COUNT;
import static org.apache.kylin.metadata.favorite.FavoriteRule.UPDATE_FREQUENCY;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.favorite.AsyncAccelerationTask;
import org.apache.kylin.metadata.favorite.AsyncTaskManager;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.candidate.RawRecManager;
import org.apache.kylin.metadata.recommendation.ref.OptRecManagerV2;
import org.apache.kylin.metadata.recommendation.ref.OptRecV2;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.AutoIndexPlanRuleUpdateRequest;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.apache.kylin.rest.response.ProjectStatisticsResponse;
import org.apache.kylin.rest.service.QueryHistoryAccelerateScheduler.QueryHistoryAccelerateRunner;
import org.apache.kylin.rest.service.util.AutoIndexPlanRuleUtil;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("projectSmartService")
public class ProjectSmartService extends BasicService implements ProjectSmartServiceSupporter {

    @Autowired
    private AclEvaluate aclEvaluate;
    @Autowired
    private UserService userService;
    @Autowired
    private ProjectSmartSupporter projectSmartSupporter;

    @Autowired
    @Qualifier("topRecsUpdateScheduler")
    TopRecsUpdateScheduler topRecsUpdateScheduler;

    @Autowired
    private ProjectModelSupporter projectModelSupporter;

    private void updateSingleRule(String project, String ruleName, FavoriteRuleUpdateRequest request) {
        List<FavoriteRule.AbstractCondition> conds = Lists.newArrayList();
        boolean isEnabled = false;
        switch (ruleName) {
        case FavoriteRule.FREQUENCY_RULE_NAME:
            isEnabled = request.isFreqEnable();
            conds.add(new FavoriteRule.Condition(null, request.getFreqValue()));
            break;
        case FavoriteRule.COUNT_RULE_NAME:
            isEnabled = request.isCountEnable();
            conds.add(new FavoriteRule.Condition(null, request.getCountValue()));
            break;
        case FavoriteRule.SUBMITTER_RULE_NAME:
            isEnabled = request.isSubmitterEnable();
            if (CollectionUtils.isNotEmpty(request.getUsers())) {
                request.getUsers().forEach(user -> conds.add(new FavoriteRule.Condition(null, user)));
            }
            break;
        case FavoriteRule.SUBMITTER_GROUP_RULE_NAME:
            isEnabled = request.isSubmitterEnable();
            if (CollectionUtils.isNotEmpty(request.getUserGroups())) {
                request.getUserGroups().forEach(userGroup -> conds.add(new FavoriteRule.Condition(null, userGroup)));
            }
            break;
        case FavoriteRule.DURATION_RULE_NAME:
            isEnabled = request.isDurationEnable();
            conds.add(new FavoriteRule.Condition(request.getMinDuration(), request.getMaxDuration()));
            break;
        case FavoriteRule.REC_SELECT_RULE_NAME:
            isEnabled = request.isRecommendationEnable();
            conds.add(new FavoriteRule.Condition(null, request.getRecommendationsValue()));
            break;
        case EFFECTIVE_DAYS:
            isEnabled = true;
            conds.add(new FavoriteRule.Condition(null, request.getEffectiveDays()));
            break;
        case UPDATE_FREQUENCY:
            isEnabled = true;
            conds.add(new FavoriteRule.Condition(null, request.getUpdateFrequency()));
            break;
        case MIN_HIT_COUNT:
            isEnabled = true;
            conds.add(new FavoriteRule.Condition(null, request.getMinHitCount()));
            break;
        case LOW_FREQUENCY_THRESHOLD:
            isEnabled = true;
            conds.add(new FavoriteRule.Condition(null, request.getLowFrequencyThreshold()));
            break;
        case FREQUENCY_TIME_WINDOW:
            isEnabled = true;
            conds.add(new FavoriteRule.Condition(null, request.getFrequencyTimeWindow()));
            break;
        default:
            break;
        }
        FavoriteRuleManager.getInstance(project).updateRule(conds, isEnabled, ruleName);
        boolean updateFrequencyChange = isChangeFreqRule(project, ruleName, request);
        if (updateFrequencyChange) {
            topRecsUpdateScheduler.reScheduleProject(project);
        }
    }

    private boolean isChangeFreqRule(String project, String ruleName, FavoriteRuleUpdateRequest request) {
        if (!UPDATE_FREQUENCY.equals(ruleName)) {
            return false;
        }
        String currentVal = FavoriteRuleManager.getInstance(project).getValue(UPDATE_FREQUENCY);
        return !currentVal.equals(request.getUpdateFrequency());
    }

    public void updateRegularRule(String project, FavoriteRuleUpdateRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
        FavoriteRuleManager manager = FavoriteRuleManager.getInstance(project);
        JdbcUtil.withTxAndRetry(manager.getTransactionManager(), () -> {
            FAVORITE_RULE_NAMES.forEach(ruleName -> {
                if (ruleName.equals(FavoriteRule.EXCLUDED_TABLES_RULE)) {
                    return;
                }
                updateSingleRule(project, ruleName, request);
            });
            return null;
        });
        NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project).listAllModels()
                .forEach(model -> projectModelSupporter.onModelUpdate(project, model.getUuid()));
    }

    public ProjectStatisticsResponse getProjectStatistics(String project) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);

        ProjectStatisticsResponse response = new ProjectStatisticsResponse();
        int[] datasourceStatistics = getDatasourceStatistics(project);
        response.setDatabaseSize(datasourceStatistics[0]);
        response.setTableSize(datasourceStatistics[1]);

        int[] recPatternCount = getRecPatternCount(project);
        response.setAdditionalRecPatternCount(recPatternCount[0]);
        response.setRemovalRecPatternCount(recPatternCount[1]);
        response.setRecPatternCount(recPatternCount[2]);

        response.setEffectiveRuleSize(getFavoriteRuleSize(project));

        int[] approvedRecsCount = getApprovedRecsCount(project);
        response.setApprovedAdditionalRecCount(approvedRecsCount[0]);
        response.setApprovedRemovalRecCount(approvedRecsCount[1]);
        response.setApprovedRecCount(approvedRecsCount[2]);

        Map<String, Set<Integer>> modelToRecMap = getModelToRecMap(project);
        response.setModelSize(modelToRecMap.size());
        if (getManager(NProjectManager.class).getProject(project).isSemiAutoMode()) {
            Set<Integer> allRecSet = Sets.newHashSet();
            modelToRecMap.values().forEach(allRecSet::addAll);
            response.setAcceptableRecSize(allRecSet.size());
            response.setMaxRecShowSize(getRecommendationSizeToShow(project));
        } else {
            response.setAcceptableRecSize(-1);
            response.setMaxRecShowSize(-1);
        }

        AsyncAccelerationTask asyncAcceleration = (AsyncAccelerationTask) AsyncTaskManager.getInstance(project)
                .get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
        Map<String, Boolean> userRefreshTag = asyncAcceleration.getUserRefreshedTagMap();
        response.setRefreshed(userRefreshTag.getOrDefault(aclEvaluate.getCurrentUserName(), false));

        return response;
    }

    public Set<Integer> accelerateManually(String project) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        Map<String, Set<Integer>> modelToRecMap = getModelToRecMap(project);

        QueryHistoryAccelerateScheduler scheduler = QueryHistoryAccelerateScheduler.getInstance();
        AsyncTaskManager manager = AsyncTaskManager.getInstance(project);
        JdbcUtil.withTxAndRetry(manager.getTransactionManager(), () -> {
            AsyncAccelerationTask accTask = getAsyncAccTask(project);
            AsyncAccelerationTask copied = manager.copyForWrite(accTask);
            copied.getUserRefreshedTagMap().put(aclEvaluate.getCurrentUserName(), false);
            manager.save(copied);
            return null;
        });

        QueryHistoryAccelerateRunner accelerateRunner = scheduler.new QueryHistoryAccelerateRunner(true, project);
        try {
            scheduler.scheduleImmediately(accelerateRunner);
            if (projectSmartSupporter != null) {
                projectSmartSupporter.onUpdateCost(project);
            }
        } catch (Throwable e) {
            log.error("Accelerate failed", e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }

        Map<String, Set<Integer>> deltaRecsMap = getDeltaRecs(modelToRecMap, project);
        Set<Integer> deltaRecSet = Sets.newHashSet();
        deltaRecsMap.forEach((k, deltaRecs) -> deltaRecSet.addAll(deltaRecs));

        JdbcUtil.withTxAndRetry(manager.getTransactionManager(), () -> {
            AsyncAccelerationTask accTask = getAsyncAccTask(project);
            AsyncAccelerationTask copied = manager.copyForWrite(accTask);
            copied.setAlreadyRunning(false);
            copied.getUserRefreshedTagMap().put(aclEvaluate.getCurrentUserName(), !deltaRecSet.isEmpty());
            manager.save(copied);
            return null;
        });
        return deltaRecSet;
    }

    public void accelerateImmediately(String project) {
        QueryHistoryAccelerateScheduler scheduler = QueryHistoryAccelerateScheduler.getInstance();
        log.info("Schedule QueryHistoryAccelerateRunner job, project [{}].", project);
        try {
            scheduler.scheduleImmediately(scheduler.new QueryHistoryAccelerateRunner(false, project));
        } catch (Exception e) {
            log.error("Accelerate failed", e);
        }
    }

    private int getFavoriteRuleSize(String project) {
        if (!getManager(NProjectManager.class).getProject(project).isSemiAutoMode()) {
            return -1;
        }
        return (int) FavoriteRuleManager.getInstance(project).listAll().stream().filter(FavoriteRule::isEnabled)
                .count();
    }

    private int[] getRecPatternCount(String project) {
        if (!getManager(NProjectManager.class).getProject(project).isSemiAutoMode()) {
            return new int[] { -1, -1, -1 };
        }
        int[] array = new int[3];
        RawRecManager recManager = RawRecManager.getInstance(project);
        Map<RawRecItem.RawRecType, Integer> recPatternCountMap = recManager.getCandidatesByProject(project);
        array[0] = recPatternCountMap.get(RawRecItem.RawRecType.ADDITIONAL_LAYOUT);
        array[1] = recPatternCountMap.get(RawRecItem.RawRecType.REMOVAL_LAYOUT);
        array[2] = array[0] + array[1];
        return array;
    }

    private int[] getDatasourceStatistics(String project) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        int[] arr = new int[2];
        NTableMetadataManager tblMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        Set<String> databaseSet = Sets.newHashSet();
        boolean streamingEnabled = getConfig().isStreamingEnabled();
        List<TableDesc> tables = tblMgr.listAllTables().stream() //
                .filter(table -> table.isAccessible(streamingEnabled)) //
                .map(table -> {
                    databaseSet.add(table.getDatabase());
                    return table;
                }).collect(Collectors.toList());
        arr[0] = databaseSet.size();
        arr[1] = tables.size();
        return arr;
    }

    private int[] getApprovedRecsCount(String project) {
        ProjectInstance projectInstance = getManager(NProjectManager.class).getProject(project);
        if (!projectInstance.isSemiAutoMode()) {
            return new int[] { -1, -1, -1 };
        }

        int[] allApprovedRecs = new int[3];
        NIndexPlanManager indexPlanManager = getManager(NIndexPlanManager.class, project);
        for (IndexPlan indexPlan : indexPlanManager.listAllIndexPlans()) {
            if (!indexPlan.isBroken()) {
                allApprovedRecs[0] += indexPlan.getApprovedAdditionalRecs();
                allApprovedRecs[1] += indexPlan.getApprovedRemovalRecs();
            }
        }
        allApprovedRecs[2] = allApprovedRecs[0] + allApprovedRecs[1];
        return allApprovedRecs;
    }

    private int getRecommendationSizeToShow(String project) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        return projectSmartSupporter != null ? projectSmartSupporter.onShowSize(project) : 0;
    }

    public AsyncAccelerationTask getAsyncAccTask(String project) {
        return (AsyncAccelerationTask) AsyncTaskManager.getInstance(project)
                .get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
    }

    public Map<String, Set<Integer>> getDeltaRecs(Map<String, Set<Integer>> modelToRecMap, String project) {
        Map<String, Set<Integer>> updatedModelToRecMap = getModelToRecMap(project);
        modelToRecMap.forEach((modelId, recSet) -> {
            if (updatedModelToRecMap.containsKey(modelId)) {
                updatedModelToRecMap.get(modelId).removeAll(recSet);
            }
        });
        updatedModelToRecMap.entrySet().removeIf(pair -> pair.getValue().isEmpty());
        return updatedModelToRecMap;
    }

    public Map<String, Set<Integer>> getModelToRecMap(String project) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);

        boolean streamingEnabled = getConfig().isStreamingEnabled();
        List<NDataModel> dataModels = getManager(NDataModelManager.class, project).listAllModels().stream()
                .filter(model -> model.isBroken() || !model.fusionModelBatchPart())
                .filter(model -> model.isAccessible(streamingEnabled)).collect(Collectors.toList());
        Map<String, Set<Integer>> map = Maps.newHashMap();
        dataModels.forEach(model -> map.putIfAbsent(model.getId(), Sets.newHashSet()));
        if (getManager(NProjectManager.class).getProject(project).isSemiAutoMode()) {
            OptRecManagerV2 optRecManager = OptRecManagerV2.getInstance(project);
            for (NDataModel model : dataModels) {
                OptRecV2 optRecV2 = optRecManager.loadOptRecV2(model.getUuid());
                map.get(model.getId()).addAll(optRecV2.getAdditionalLayoutRefs().keySet());
                map.get(model.getId()).addAll(optRecV2.getRemovalLayoutRefs().keySet());
            }
        }
        return map;
    }

    @Override
    public Map<String, Object> getFavoriteRules(String project) {
        Map<String, Object> result = Maps.newHashMap();

        for (String ruleName : FAVORITE_RULE_NAMES) {
            getSingleRule(project, ruleName, result);
        }

        return result;
    }

    private void getSingleRule(String project, String ruleName, Map<String, Object> result) {
        FavoriteRule rule = getFavoriteRule(project, ruleName);
        List<FavoriteRule.Condition> conditions = rule.getConds().stream()
                .filter(condition -> condition instanceof FavoriteRule.Condition)
                .map(condition -> (FavoriteRule.Condition) condition).collect(Collectors.toList());
        String left = null;
        String right = null;
        if (conditions.size() == 1) {
            FavoriteRule.Condition condition = conditions.get(0);
            left = condition.getLeftThreshold();
            right = condition.getRightThreshold();
        }

        switch (ruleName) {
        case FavoriteRule.FREQUENCY_RULE_NAME:
            result.put("freq_enable", rule.isEnabled());
            result.put("freq_value", parseFloat(right));
            break;
        case FavoriteRule.COUNT_RULE_NAME:
            result.put("count_enable", rule.isEnabled());
            result.put("count_value", parseFloat(right));
            break;
        case FavoriteRule.SUBMITTER_RULE_NAME:
            result.put("submitter_enable", rule.isEnabled());
            result.put("users", getUsers(conditions));
            break;
        case FavoriteRule.SUBMITTER_GROUP_RULE_NAME:
            result.put("user_groups", getUserGroups(conditions));
            break;
        case FavoriteRule.DURATION_RULE_NAME:
            result.put("duration_enable", rule.isEnabled());
            result.put("min_duration", parseLong(left));
            result.put("max_duration", parseLong(right));
            break;
        case FavoriteRule.REC_SELECT_RULE_NAME:
            result.put("recommendation_enable", rule.isEnabled());
            result.put("recommendations_value", parseLong(right));
            break;
        case MIN_HIT_COUNT:
            result.put("min_hit_count", parseInt(right));
            break;
        case EFFECTIVE_DAYS:
            result.put("effective_days", parseInt(right));
            break;
        case UPDATE_FREQUENCY:
            result.put("update_frequency", parseInt(right));
            break;
        case FREQUENCY_TIME_WINDOW:
            result.put("frequency_time_window", right);
            break;
        case LOW_FREQUENCY_THRESHOLD:
            result.put("low_frequency_threshold", parseInt(right));
            break;
        default:
            break;
        }
    }

    private List<String> getUsers(List<FavoriteRule.Condition> conditionList) {
        List<String> users = Lists.newArrayList();
        conditionList.forEach(cond -> {
            if (Constant.ADMIN.equalsIgnoreCase(cond.getRightThreshold())
                    || userService.userExists(cond.getRightThreshold())) {
                users.add(cond.getRightThreshold());
            }
        });
        return users;
    }

    @SneakyThrows(IOException.class)
    private List<String> getUserGroups(List<FavoriteRule.Condition> conditionList) {
        List<String> userGroups = Lists.newArrayList();
        for (FavoriteRule.Condition cond : conditionList) {
            if (Constant.ROLE_ADMIN.equalsIgnoreCase(cond.getRightThreshold())
                    || userGroupService.exists(cond.getRightThreshold())) {
                userGroups.add(cond.getRightThreshold());
            }
        }
        return userGroups;
    }

    private Integer parseInt(String str) {
        return StringUtils.isEmpty(str) ? null : Integer.parseInt(str);
    }

    private Float parseFloat(String str) {
        return StringUtils.isEmpty(str) ? null : Float.parseFloat(str);
    }

    private Long parseLong(String str) {
        return StringUtils.isEmpty(str) ? null : Long.parseLong(str);
    }

    private FavoriteRule getFavoriteRule(String project, String ruleName) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        Preconditions.checkArgument(StringUtils.isNotEmpty(ruleName));

        return FavoriteRuleManager.getInstance(project).getOrDefaultByName(ruleName);
    }

    @Override
    public Map<String, Object> getAutoIndexPlanRule(String project) {
        Map<String, Object> result = Maps.newHashMap();
        FavoriteRuleManager manager = FavoriteRuleManager.getInstance(project);
        manager.listAutoIndexPlanRules().forEach(rule -> {
            result.put(rule.getName(), AutoIndexPlanRuleUtil.getRuleValue(rule));
        });
        if (!isEnableAutoSemi(project)) {
            result.put(FavoriteRule.INDEX_PLANNER_ENABLE, false);
        }
        return result;
    }

    public boolean isEnableAutoSemi(String project) {
        val projectInstance = getManager(NProjectManager.class).getProject(project);
        val config = projectInstance.getConfig();
        return config.isSemiAutoMode();
    }

    public void updateAutoIndexPlanRule(String project, AutoIndexPlanRuleUpdateRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
        FavoriteRuleManager manager = FavoriteRuleManager.getInstance(project);
        JdbcUtil.withTxAndRetry(manager.getTransactionManager(), () -> {
            AUTO_INDEX_PLAN_RULE_NAMES.stream()
                    .filter(ruleName -> !ruleName.equals(FavoriteRule.AUTO_INDEX_PLAN_OPTION)).forEach(ruleName -> {
                        List<FavoriteRule.AbstractCondition> conds = AutoIndexPlanRuleUtil
                                .getConditionsFromUpdateRequest(ruleName, request);
                        manager.updateRule(conds, true, ruleName);
                    });
            return null;
        });
    }

}

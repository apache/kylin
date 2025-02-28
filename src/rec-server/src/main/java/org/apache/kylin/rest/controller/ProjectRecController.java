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

package org.apache.kylin.rest.controller;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_COUNT_RULE_VALUE;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_DURATION_RULE_VALUE;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_EFFECTIVE_DAYS;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_FREQUENCY_RULE_VALUE;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_MIN_HIT_COUNT;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_REC_RULE_VALUE;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_UPDATE_FREQUENCY;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_DATE_UNIT;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_RANGE;
import static org.apache.kylin.metadata.favorite.FavoriteRule.EFFECTIVE_DAYS_MAX;
import static org.apache.kylin.metadata.favorite.FavoriteRule.EFFECTIVE_DAYS_MIN;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.asynctask.AbstractAsyncTask;
import org.apache.kylin.metadata.favorite.AsyncAccelerationTask;
import org.apache.kylin.metadata.favorite.AsyncTaskManager;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.rest.aspect.WaitForSyncBeforeRPC;
import org.apache.kylin.rest.request.AutoIndexPlanRuleUpdateRequest;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ProjectStatisticsResponse;
import org.apache.kylin.rest.service.ProjectSmartService;
import org.apache.kylin.rest.service.QueryHistoryService;
import org.apache.kylin.rest.service.util.AutoIndexPlanRuleUtil;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/projects", produces = { HTTP_VND_APACHE_KYLIN_JSON,
        HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class ProjectRecController extends NBasicController {

    @Autowired
    @Qualifier("projectSmartService")
    private ProjectSmartService projectSmartService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("queryHistoryService")
    private QueryHistoryService qhService;

    protected static void checkUpdateFavoriteRuleArgs(FavoriteRuleUpdateRequest request) {
        // either disabled or arguments not empty
        if (request.isFreqEnable() && StringUtils.isEmpty(request.getFreqValue())) {
            throw new KylinException(EMPTY_FREQUENCY_RULE_VALUE, MsgPicker.getMsg().getFrequencyThresholdCanNotEmpty());
        }
        if (request.isDurationEnable()
                && (StringUtils.isEmpty(request.getMinDuration()) || StringUtils.isEmpty(request.getMaxDuration()))) {
            throw new KylinException(EMPTY_DURATION_RULE_VALUE, MsgPicker.getMsg().getDelayThresholdCanNotEmpty());
        }
        if (request.isCountEnable() && StringUtils.isEmpty(request.getCountValue())) {
            throw new KylinException(EMPTY_COUNT_RULE_VALUE, MsgPicker.getMsg().getFrequencyThresholdCanNotEmpty());
        }
        if (request.isRecommendationEnable() && StringUtils.isEmpty(request.getRecommendationsValue().trim())) {
            throw new KylinException(EMPTY_REC_RULE_VALUE, MsgPicker.getMsg().getRecommendationLimitNotEmpty());
        }
        if (StringUtils.isEmpty(request.getMinHitCount())) {
            throw new KylinException(EMPTY_MIN_HIT_COUNT, MsgPicker.getMsg().getMinHitCountNotEmpty());
        }
        if (StringUtils.isEmpty(request.getEffectiveDays())) {
            throw new KylinException(EMPTY_EFFECTIVE_DAYS, MsgPicker.getMsg().getEffectiveDaysNotEmpty());
        }
        if (StringUtils.isEmpty(request.getUpdateFrequency())) {
            throw new KylinException(EMPTY_UPDATE_FREQUENCY, MsgPicker.getMsg().getUpdateFrequencyNotEmpty());
        }
        checkRange(request.getRecommendationsValue(), 0, Integer.MAX_VALUE);
        checkRange(request.getMinHitCount(), 1, Integer.MAX_VALUE);
        checkRange(request.getEffectiveDays(), EFFECTIVE_DAYS_MIN, EFFECTIVE_DAYS_MAX);
        checkRange(request.getUpdateFrequency(), 1, Integer.MAX_VALUE);
        checkRange(request.getLowFrequencyThreshold(), 0, Integer.MAX_VALUE);
        if (!FavoriteRule.DATE_UNIT_CANDIDATES.contains(request.getFrequencyTimeWindow())) {
            throw new KylinException(INVALID_DATE_UNIT, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getInvalidDateUnit(), request.getFrequencyTimeWindow()));
        }
    }

    private static void checkRange(String value, int start, int end) {
        boolean inRightRange;
        try {
            int i = Integer.parseInt(value);
            inRightRange = (i >= start && i <= end);
        } catch (Exception e) {
            inRightRange = false;
        }

        if (!inRightRange) {
            throw new KylinException(INVALID_RANGE,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getInvalidRange(), value, start, end));
        }
    }

    @ApiOperation(value = "getFavoriteRules", tags = {
            "SM" }, notes = "Update Param: freq_enable, freq_value, count_enable, count_value, duration_enable, min_duration, max_duration, submitter_enable, user_groups")
    @GetMapping(value = "/{project:.+}/favorite_rules")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public EnvelopeResponse<Map<String, Object>> getFavoriteRules(@PathVariable(value = "project") String project) {
        checkProjectName(project);
        aclEvaluate.checkProjectWritePermission(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, projectSmartService.getFavoriteRules(project), "");
    }

    @ApiOperation(value = "statistics", tags = { "SM" })
    @GetMapping(value = "/statistics")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public EnvelopeResponse<ProjectStatisticsResponse> getDashboardStatistics(@RequestParam("project") String project) {
        checkProjectName(project);
        ProjectStatisticsResponse projectStatistics = projectSmartService.getProjectStatistics(project);
        projectStatistics.setLastWeekQueryCount(qhService.getLastWeekQueryCount(project));
        projectStatistics.setUnhandledQueryCount(qhService.getQueryCountToAccelerate(project));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, projectStatistics, "");
    }

    @ApiOperation(value = "getAcceleration", tags = { "AI" })
    @GetMapping(value = "/acceleration")
    @ResponseBody
    public EnvelopeResponse<Boolean> isAccelerating(@RequestParam("project") String project) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        AbstractAsyncTask asyncTask = AsyncTaskManager.getInstance(project)
                .get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                ((AsyncAccelerationTask) asyncTask).isAlreadyRunning(), "");
    }

    @ApiOperation(value = "updateAcceleration", tags = { "AI" })
    @PutMapping(value = "/acceleration")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public EnvelopeResponse<Object> accelerate(@RequestParam("project") String project) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        Set<Integer> deltaRecs = projectSmartService.accelerateManually(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, deltaRecs.size(), "");
    }

    @ApiOperation(value = "statistics", tags = { "AI" })
    @PostMapping(value = "/acceleration_tag")
    @ResponseBody
    public EnvelopeResponse<Object> cleanAsyncAccelerateTag(@RequestParam("project") String project,
            @RequestParam("user") String user) {
        checkProjectName(project);
        checkRequiredArg("user", user);
        AsyncTaskManager.cleanAccelerationTagByUser(project, user);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }

    @ApiOperation(value = "updateFavoriteRules", tags = {
            "SM" }, notes = "Update Param: freq_enable, freq_value, count_enable, count_value, duration_enable, min_duration, max_duration, submitter_enable, user_groups")
    @PutMapping(value = "/{project:.+}/favorite_rules")
    @ResponseBody
    public EnvelopeResponse<String> updateFavoriteRules(@RequestBody FavoriteRuleUpdateRequest request) {
        checkProjectName(request.getProject());
        checkProjectUnmodifiable(request.getProject());
        checkUpdateFavoriteRuleArgs(request);
        projectSmartService.updateRegularRule(request.getProject(), request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    // ---------------------- Deprecated It will be deleted later ----------------------
    @ApiOperation(value = "updateAutoIndexPlanRules", tags = { "AI" })
    @PutMapping(value = "/{project:.+}/auto_index_plan_rule")
    @ResponseBody
    public EnvelopeResponse<String> updateAutoIndexPlanRules(@PathVariable(value = "project") String project,
            @RequestBody AutoIndexPlanRuleUpdateRequest request) {
        checkProjectName(project);
        checkProjectUnmodifiable(project);
        request.setProject(project);
        AutoIndexPlanRuleUtil.checkUpdateFavoriteRuleArgs(request);
        projectSmartService.updateAutoIndexPlanRule(request.getProject(), request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateIndexPlannerRules", tags = { "AI" })
    @PutMapping(value = "/index_planner_rule")
    @ResponseBody
    public EnvelopeResponse<String> updateIndexPlannerRules(@RequestBody AutoIndexPlanRuleUpdateRequest request) {
        String project = request.getProject();
        checkProjectName(project);
        checkProjectUnmodifiable(project);
        request.setProject(project);
        checkRequiredArg("indexPlannerEnable", request.isIndexPlannerEnable());
        AutoIndexPlanRuleUtil.checkUpdateFavoriteRuleArgs(request);
        projectSmartService.updateAutoIndexPlanRule(request.getProject(), request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

}

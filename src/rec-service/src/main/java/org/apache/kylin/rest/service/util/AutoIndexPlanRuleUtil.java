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

package org.apache.kylin.rest.service.util;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_AUTO_COMPLETE_MODE;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_DATE_FORMAT;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_DATE_UNIT;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_RANGE;
import static org.apache.kylin.common.exception.ServerErrorCode.SEMI_AUTO_NOT_ENABLED;

import java.time.LocalDate;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.request.AutoIndexPlanRuleUpdateRequest;

public class AutoIndexPlanRuleUtil {

    private AutoIndexPlanRuleUtil() {

    }

    public static void checkUpdateFavoriteRuleArgs(AutoIndexPlanRuleUpdateRequest request) {
        if (!request.isIndexPlannerEnable()) {
            return;
        }
        AutoIndexPlanRuleUtil.checkRange(request.getAutoIndexPlanOption(), 0, 2);
        // semi-mode should be turn-on before enable auto-index-plan
        KylinConfig config = NProjectManager.getProjectConfig(request.getProject());
        if (!config.isSemiAutoMode()) {
            throw new KylinException(SEMI_AUTO_NOT_ENABLED, MsgPicker.getMsg().getSemiAutoNotEnabled());
        }
        if (!FavoriteRule.AUTO_COMPLETE_MODES.contains(request.getAutoIndexPlanAutoCompleteMode())) {
            throw new KylinException(INVALID_AUTO_COMPLETE_MODE, MsgPicker.getMsg().getAutoCompleteModeNotValid());
        }
        if ("ABSOLUTE".equals(request.getAutoIndexPlanAutoCompleteMode())) {
            checkDateFormat(request.getAutoIndexPlanAbsoluteBeginDate());
        } else {
            if (!FavoriteRule.DATE_UNIT_CANDIDATES.contains(request.getAutoIndexPlanRelativeTimeUnit())) {
                throw new KylinException(INVALID_DATE_UNIT, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getInvalidDateUnit(), request.getAutoIndexPlanRelativeTimeUnit()));
            }
            AutoIndexPlanRuleUtil.checkRange(request.getAutoIndexPlanRelativeTimeInterval(), 0, 1000);
        }
        AutoIndexPlanRuleUtil.checkRange(request.getIndexPlannerMaxIndexCount(), 1, 100000);
        AutoIndexPlanRuleUtil.checkRange(request.getIndexPlannerMaxChangeCount(), 0,
                Math.min(request.getIndexPlannerMaxIndexCount(), 100));
    }

    private static void checkDateFormat(String value) {
        boolean isRightFormat = true;
        if (value == null) {
            isRightFormat = false;
        } else {
            try {
                LocalDate.parse(value);
            } catch (Exception e) {
                isRightFormat = false;
            }
        }
        if (!isRightFormat) {
            throw new KylinException(INVALID_DATE_FORMAT,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getInvalidDateFormat(), "yyyy-MM-dd", value));
        }
    }

    private static void checkRange(Integer value, int start, int end) {
        boolean inRightRange;
        if (value == null) {
            inRightRange = false;
        } else {
            inRightRange = (value >= start && value <= end);
        }
        if (!inRightRange) {
            throw new KylinException(INVALID_RANGE,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getInvalidRange(), value, start, end));
        }
    }

    public static Object getRuleValue(FavoriteRule rule) {
        if (rule == null) {
            return null;
        }
        List<FavoriteRule.Condition> conditions = rule.getConds().stream()
                .filter(condition -> condition instanceof FavoriteRule.Condition)
                .map(condition -> (FavoriteRule.Condition) condition).collect(Collectors.toList());
        String right = null;
        if (conditions.size() == 1) {
            FavoriteRule.Condition condition = conditions.get(0);
            right = condition.getRightThreshold();
        }
        String ruleName = rule.getName();
        switch (ruleName) {
        case FavoriteRule.AUTO_INDEX_PLAN_OPTION:
            return getInteger(right, 0);
        case FavoriteRule.INDEX_PLANNER_ENABLE:
            return getBoolean(right, null);
        case FavoriteRule.AUTO_INDEX_PLAN_SEGMENT_JOB_ENABLE:
        case FavoriteRule.AUTO_INDEX_PLAN_AUTO_CHANGE_INDEX_ENABLE:
            return getBoolean(right, true);
        case FavoriteRule.INDEX_PLANNER_MAX_INDEX_COUNT:
            return getInteger(right, 100);
        case FavoriteRule.INDEX_PLANNER_MAX_CHANGE_COUNT:
            return getInteger(right, 10);
        case FavoriteRule.INDEX_PLANNER_LEVEL:
            return right == null ? FavoriteRule.IndexPlannerLevelType.BALANCED.name() : right;
        case FavoriteRule.AUTO_INDEX_PLAN_AUTO_COMPLETE_MODE:
            return right == null ? "ABSOLUTE" : right;
        case FavoriteRule.AUTO_INDEX_PLAN_RELATIVE_TIME_INTERVAL:
            return getInteger(right, 12);
        case FavoriteRule.AUTO_INDEX_PLAN_RELATIVE_TIME_UNIT:
            return right == null ? "MONTH" : right;
        case FavoriteRule.AUTO_INDEX_PLAN_ABSOLUTE_BEGIN_DATE:
            return right;
        default:
            break;
        }
        return null;
    }

    private static Boolean getBoolean(String value, Boolean defaultValue) {
        if (StringUtils.isBlank(value) || StringUtils.equals(value, "null")) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    private static Integer getInteger(String value, Integer defaultValue) {
        if (StringUtils.isBlank(value) || StringUtils.equals(value, "null")) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    public static List<FavoriteRule.AbstractCondition> getConditionsFromUpdateRequest(String ruleName,
            AutoIndexPlanRuleUpdateRequest request) {
        List<FavoriteRule.AbstractCondition> conds = Lists.newArrayList();
        switch (ruleName) {
        case FavoriteRule.AUTO_INDEX_PLAN_OPTION:
            conds.add(new FavoriteRule.Condition(null, Objects.toString(request.getAutoIndexPlanOption())));
            break;
        case FavoriteRule.INDEX_PLANNER_ENABLE:
            conds.add(new FavoriteRule.Condition(null, Boolean.toString(request.isIndexPlannerEnable())));
            break;
        case FavoriteRule.AUTO_INDEX_PLAN_AUTO_CHANGE_INDEX_ENABLE:
            conds.add(
                    new FavoriteRule.Condition(null, Boolean.toString(request.isAutoIndexPlanAutoChangeIndexEnable())));
            break;
        case FavoriteRule.INDEX_PLANNER_MAX_INDEX_COUNT:
            conds.add(new FavoriteRule.Condition(null, Objects.toString(request.getIndexPlannerMaxIndexCount())));
            break;
        case FavoriteRule.INDEX_PLANNER_MAX_CHANGE_COUNT:
            conds.add(new FavoriteRule.Condition(null, Objects.toString(request.getIndexPlannerMaxChangeCount())));
            break;
        case FavoriteRule.INDEX_PLANNER_LEVEL:
            conds.add(new FavoriteRule.Condition(null, Objects.toString(request.getIndexPlannerLevel())));
            break;
        case FavoriteRule.AUTO_INDEX_PLAN_AUTO_COMPLETE_MODE:
            conds.add(new FavoriteRule.Condition(null, request.getAutoIndexPlanAutoCompleteMode()));
            break;
        case FavoriteRule.AUTO_INDEX_PLAN_RELATIVE_TIME_INTERVAL:
            conds.add(
                    new FavoriteRule.Condition(null, Objects.toString(request.getAutoIndexPlanRelativeTimeInterval())));
            break;
        case FavoriteRule.AUTO_INDEX_PLAN_RELATIVE_TIME_UNIT:
            conds.add(new FavoriteRule.Condition(null, request.getAutoIndexPlanRelativeTimeUnit()));
            break;
        case FavoriteRule.AUTO_INDEX_PLAN_ABSOLUTE_BEGIN_DATE:
            conds.add(new FavoriteRule.Condition(null, request.getAutoIndexPlanAbsoluteBeginDate()));
            break;
        case FavoriteRule.AUTO_INDEX_PLAN_SEGMENT_JOB_ENABLE:
            conds.add(new FavoriteRule.Condition(null, Boolean.toString(request.isAutoIndexPlanSegmentJobEnable())));
            break;
        default:
            break;
        }
        return conds;
    }
}

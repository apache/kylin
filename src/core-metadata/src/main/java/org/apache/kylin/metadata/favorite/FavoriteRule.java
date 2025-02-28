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

package org.apache.kylin.metadata.favorite;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public class FavoriteRule {

    public static final String FREQUENCY_RULE_NAME = "frequency";
    public static final String COUNT_RULE_NAME = "count";
    public static final String DURATION_RULE_NAME = "duration";
    public static final String SUBMITTER_RULE_NAME = "submitter";
    public static final String SUBMITTER_GROUP_RULE_NAME = "submitter_group";
    public static final String REC_SELECT_RULE_NAME = "recommendations";
    public static final String EXCLUDED_TABLES_RULE = "excluded_tables";
    public static final String MIN_HIT_COUNT = "min_hit_count";
    public static final String EFFECTIVE_DAYS = "effective_days";
    public static final String UPDATE_FREQUENCY = "update_frequency";
    public static final String FREQUENCY_TIME_WINDOW = "frequency_time_window";
    public static final String LOW_FREQUENCY_THRESHOLD = "low_frequency_threshold";
    public static final int EFFECTIVE_DAYS_MIN = 1;
    public static final int EFFECTIVE_DAYS_MAX = 30;

    // ------------------------------- core confs -------------------------------
    public static final String INDEX_PLANNER_ENABLE = "index_planner_enable";
    public static final String INDEX_PLANNER_MAX_INDEX_COUNT = "index_planner_max_index_count";
    public static final String INDEX_PLANNER_MAX_CHANGE_COUNT = "index_planner_max_change_count";
    public static final String INDEX_PLANNER_LEVEL = "index_planner_level";

    // ------------------------------- Deprecated It will be deleted later-------------------------------
    public static final String AUTO_INDEX_PLAN_OPTION = "auto_index_plan_option";
    public static final String AUTO_INDEX_PLAN_AUTO_CHANGE_INDEX_ENABLE = "auto_index_plan_auto_change_index_enable";
    public static final String AUTO_INDEX_PLAN_AUTO_COMPLETE_MODE = "auto_index_plan_auto_complete_mode";
    public static final String AUTO_INDEX_PLAN_ABSOLUTE_BEGIN_DATE = "auto_index_plan_absolute_begin_date";
    public static final String AUTO_INDEX_PLAN_RELATIVE_TIME_UNIT = "auto_index_plan_relative_time_unit";
    public static final String AUTO_INDEX_PLAN_RELATIVE_TIME_INTERVAL = "auto_index_plan_relative_time_interval";
    public static final String AUTO_INDEX_PLAN_SEGMENT_JOB_ENABLE = "auto_index_plan_segment_job_enable";

    public static final List<String> FAVORITE_RULE_NAMES = ImmutableList.of(FavoriteRule.COUNT_RULE_NAME,
            FavoriteRule.FREQUENCY_RULE_NAME, FavoriteRule.DURATION_RULE_NAME, FavoriteRule.SUBMITTER_RULE_NAME,
            FavoriteRule.SUBMITTER_GROUP_RULE_NAME, FavoriteRule.REC_SELECT_RULE_NAME,
            FavoriteRule.EXCLUDED_TABLES_RULE, MIN_HIT_COUNT, EFFECTIVE_DAYS, UPDATE_FREQUENCY, FREQUENCY_TIME_WINDOW,
            LOW_FREQUENCY_THRESHOLD);

    public static final List<String> AUTO_INDEX_PLAN_RULE_NAMES = ImmutableList.of(AUTO_INDEX_PLAN_OPTION,
            INDEX_PLANNER_ENABLE, INDEX_PLANNER_MAX_INDEX_COUNT, INDEX_PLANNER_MAX_CHANGE_COUNT, INDEX_PLANNER_LEVEL,
            AUTO_INDEX_PLAN_AUTO_CHANGE_INDEX_ENABLE, AUTO_INDEX_PLAN_AUTO_COMPLETE_MODE,
            AUTO_INDEX_PLAN_ABSOLUTE_BEGIN_DATE, AUTO_INDEX_PLAN_RELATIVE_TIME_UNIT,
            AUTO_INDEX_PLAN_RELATIVE_TIME_INTERVAL, AUTO_INDEX_PLAN_SEGMENT_JOB_ENABLE);

    public static final List<String> INDEX_PLANNER_RULE_NAMES = ImmutableList.of(AUTO_INDEX_PLAN_OPTION,
            INDEX_PLANNER_ENABLE, INDEX_PLANNER_MAX_INDEX_COUNT, INDEX_PLANNER_MAX_CHANGE_COUNT, INDEX_PLANNER_LEVEL);

    public static final List<String> AUTO_COMPLETE_MODES = Arrays.stream(AutoCompleteModeType.values()).map(Enum::name)
            .collect(Collectors.toList());
    public static final List<String> DATE_UNIT_CANDIDATES = Arrays.stream(DateUnitType.values()).map(Enum::name)
            .collect(Collectors.toList());

    private int id;
    private String project;
    private String model;
    private List<AbstractCondition> conds = Lists.newArrayList();
    private String name;
    private boolean enabled;
    private long updateTime;
    private long createTime;
    private long mvcc;

    public FavoriteRule(List<AbstractCondition> conds, String name, boolean isEnabled) {
        this.conds = conds;
        this.name = name;
        this.enabled = isEnabled;
    }

    public static List<FavoriteRule> getRecommendDefaultRule() {
        return FAVORITE_RULE_NAMES.stream().map(ruleName -> getDefaultRuleIfNull(null, ruleName))
                .collect(Collectors.toList());
    }

    public static List<FavoriteRule> getAutoIndexPlanDefaultRule() {
        return AUTO_INDEX_PLAN_RULE_NAMES.stream().map(ruleName -> getDefaultRuleIfNull(null, ruleName))
                .collect(Collectors.toList());
    }

    public static FavoriteRule getDefaultRuleIfNull(FavoriteRule rule, String name) {
        if (AUTO_INDEX_PLAN_RULE_NAMES.contains(name)) {
            return rule == null
                    ? new FavoriteRule(Lists.newArrayList(getDefaultAutoIndexPlanCondition(name)), name, true)
                    : rule;
        }
        switch (name) {
        case COUNT_RULE_NAME:
        case SUBMITTER_GROUP_RULE_NAME:
        case SUBMITTER_RULE_NAME:
        case REC_SELECT_RULE_NAME:
        case MIN_HIT_COUNT:
        case UPDATE_FREQUENCY:
        case EFFECTIVE_DAYS:
        case DURATION_RULE_NAME:
        case FREQUENCY_TIME_WINDOW:
        case LOW_FREQUENCY_THRESHOLD:
            return rule == null ? new FavoriteRule(Lists.newArrayList(getDefaultCondition(name)), name, true) : rule;
        case FREQUENCY_RULE_NAME:
        case EXCLUDED_TABLES_RULE:
            return rule == null ? new FavoriteRule(Lists.newArrayList(getDefaultCondition(name)), name, false) : rule;
        default:
            return rule;
        }
    }

    public static Condition getDefaultCondition(String ruleName) {
        switch (ruleName) {
        case COUNT_RULE_NAME:
            return new Condition(null, "10");
        case FREQUENCY_RULE_NAME:
            return new Condition(null, "0.1");
        case SUBMITTER_RULE_NAME:
            return new Condition(null, "ADMIN");
        case SUBMITTER_GROUP_RULE_NAME:
            return new Condition(null, "ROLE_ADMIN");
        case DURATION_RULE_NAME:
            return new Condition("5", "3600");
        case REC_SELECT_RULE_NAME:
            return new Condition(null, "20");
        case EXCLUDED_TABLES_RULE:
            return new Condition(null, "");
        case MIN_HIT_COUNT:
            return new Condition(null, "30");
        case UPDATE_FREQUENCY:
        case EFFECTIVE_DAYS:
            return new Condition(null, "2");
        case LOW_FREQUENCY_THRESHOLD:
            return new Condition(null, "0");
        case FREQUENCY_TIME_WINDOW:
            return new Condition(null, DateUnitType.MONTH.name());
        default:
            return null;
        }
    }

    public static Condition getDefaultAutoIndexPlanCondition(String ruleName) {
        switch (ruleName) {
        case INDEX_PLANNER_ENABLE:
            return new Condition(null, "false");
        case INDEX_PLANNER_MAX_INDEX_COUNT:
            return new Condition(null, "100");
        case INDEX_PLANNER_MAX_CHANGE_COUNT:
            return new Condition(null, "10");
        case INDEX_PLANNER_LEVEL:
            return new Condition(null, IndexPlannerLevelType.BALANCED.name());
        // -------- Deprecated It will be deleted later --------
        case AUTO_INDEX_PLAN_SEGMENT_JOB_ENABLE:
        case AUTO_INDEX_PLAN_AUTO_CHANGE_INDEX_ENABLE:
            return new Condition(null, "true");
        case AUTO_INDEX_PLAN_AUTO_COMPLETE_MODE:
            return new Condition(null, "RELATIVE");
        case AUTO_INDEX_PLAN_RELATIVE_TIME_UNIT:
            return new Condition(null, DateUnitType.MONTH.name());
        case AUTO_INDEX_PLAN_RELATIVE_TIME_INTERVAL:
            return new Condition(null, "12");
        case AUTO_INDEX_PLAN_ABSOLUTE_BEGIN_DATE:
            return new Condition(null, null);
        default:
            return null;
        }
    }

    public enum DateUnitType {
        DAY, WEEK, MONTH, QUARTER, YEAR
    }

    public enum AutoCompleteModeType {
        ABSOLUTE, RELATIVE
    }

    public enum IndexPlannerLevelType {
        HIGH_PERFORMANCE, BALANCED, LOW_COST
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
    @JsonSubTypes({ @JsonSubTypes.Type(value = Condition.class), @JsonSubTypes.Type(value = SQLCondition.class) })
    @NoArgsConstructor
    public abstract static class AbstractCondition implements Serializable {
    }

    @Getter
    @Setter
    @NoArgsConstructor
    public static class Condition extends AbstractCondition {
        private String leftThreshold;
        private String rightThreshold;

        public Condition(String leftThreshold, String rightThreshold) {
            this.leftThreshold = leftThreshold;
            this.rightThreshold = rightThreshold;
        }
    }

    @Getter
    @Setter
    public static class SQLCondition extends AbstractCondition {
        private String id;
        @JsonProperty("sql_pattern")
        private String sqlPattern;
        @JsonProperty("create_time")
        private long createTime;

        public SQLCondition() {
            this.id = RandomUtil.randomUUIDStr();
        }

        public SQLCondition(String sqlPattern) {
            this();
            this.sqlPattern = sqlPattern;
            this.createTime = System.currentTimeMillis();
        }

        @VisibleForTesting
        public SQLCondition(String id, String sqlPattern) {
            this.id = id;
            this.sqlPattern = sqlPattern;
            this.createTime = System.currentTimeMillis();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null)
                return false;

            if (this.getClass() != obj.getClass())
                return false;

            SQLCondition that = (SQLCondition) obj;
            return this.sqlPattern.equalsIgnoreCase(that.getSqlPattern());
        }

        @Override
        public int hashCode() {
            return this.sqlPattern.hashCode();
        }
    }

    public static int getTimeWindowLength(String timeUnit) {
        switch (timeUnit) {
        case "DAY":
            return 1;
        case "WEEK":
            return 7;
        case "MONTH":
            return 30;
        case "QUARTER":
            return 90;
        default:
            throw new KylinException(INVALID_PARAMETER, "Illegal parameter 'frequency_time_window'!");
        }
    }
}

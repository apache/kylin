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

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.RandomUtil;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public class FavoriteRule extends RootPersistentEntity {

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
    public static final int EFFECTIVE_DAYS_MIN = 1;
    public static final int EFFECTIVE_DAYS_MAX = 30;

    public static final List<String> FAVORITE_RULE_NAMES = ImmutableList.of(FavoriteRule.COUNT_RULE_NAME,
            FavoriteRule.FREQUENCY_RULE_NAME, FavoriteRule.DURATION_RULE_NAME, FavoriteRule.SUBMITTER_RULE_NAME,
            FavoriteRule.SUBMITTER_GROUP_RULE_NAME, FavoriteRule.REC_SELECT_RULE_NAME,
            FavoriteRule.EXCLUDED_TABLES_RULE, MIN_HIT_COUNT, EFFECTIVE_DAYS, UPDATE_FREQUENCY);

    public FavoriteRule(List<AbstractCondition> conds, String name, boolean isEnabled) {
        this.conds = conds;
        this.name = name;
        this.enabled = isEnabled;
    }

    @JsonProperty("conds")
    private List<AbstractCondition> conds = Lists.newArrayList();
    @JsonProperty("name")
    private String name;
    @JsonProperty("enabled")
    private boolean enabled;

    public static List<FavoriteRule> getAllDefaultRule() {
        return FAVORITE_RULE_NAMES.stream().map(ruleName -> getDefaultRuleIfNull(null, ruleName))
                .collect(Collectors.toList());
    }

    public static FavoriteRule getDefaultRuleIfNull(FavoriteRule rule, String name) {
        switch (name) {
        case COUNT_RULE_NAME:
        case SUBMITTER_GROUP_RULE_NAME:
        case SUBMITTER_RULE_NAME:
        case REC_SELECT_RULE_NAME:
        case MIN_HIT_COUNT:
        case UPDATE_FREQUENCY:
        case EFFECTIVE_DAYS:
            return rule == null ? new FavoriteRule(Lists.newArrayList(getDefaultCondition(name)), name, true) : rule;
        case FREQUENCY_RULE_NAME:
        case DURATION_RULE_NAME:
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
            return new Condition("0", "180");
        case REC_SELECT_RULE_NAME:
            return new Condition(null, "20");
        case EXCLUDED_TABLES_RULE:
            return new Condition(null, "");
        case MIN_HIT_COUNT:
            return new Condition(null, "30");
        case UPDATE_FREQUENCY:
        case EFFECTIVE_DAYS:
            return new Condition(null, "2");
        default:
            return null;
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
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
}

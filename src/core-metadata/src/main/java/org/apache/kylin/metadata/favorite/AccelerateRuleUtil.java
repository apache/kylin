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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryHistoryInfo;

import lombok.var;

@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public class AccelerateRuleUtil {

    static class InternalBlackOutRule {

        private static InternalBlackOutRule instance;

        public static synchronized InternalBlackOutRule getSingletonInstance() {
            if (instance == null) {
                instance = new InternalBlackOutRule();
            }

            return instance;
        }

        private InternalBlackOutRule() {

        }

        /**
         * TRUE --> reserve query history
         * FALSE --> discard query history
         *
         * @param queryHistory
         * @return
         */
        public boolean filterCannotAccelerate(QueryHistory queryHistory) {
            return !exactlyMatchedQuery(queryHistory) && !pushdownForExecutionError(queryHistory);
        }

        private boolean exactlyMatchedQuery(QueryHistory queryHistory) {
            return queryHistory.getQueryHistoryInfo().isExactlyMatch();
        }

        private boolean pushdownForExecutionError(QueryHistory queryHistory) {
            return queryHistory.getQueryHistoryInfo().isExecutionError();
        }
    }

    public List<QueryHistory> findMatchedCandidate(String project, List<QueryHistory> queryHistories,
            Map<String, Set<String>> submitterToGroups, List<Pair<Long, QueryHistoryInfo>> batchArgs) {
        List<QueryHistory> candidate = Lists.newArrayList();
        for (QueryHistory qh : queryHistories) {
            QueryHistoryInfo queryHistoryInfo = qh.getQueryHistoryInfo();
            if (queryHistoryInfo == null) {
                continue;
            }
            if (matchCustomerRule(qh, project, submitterToGroups) && matchInternalRule(qh)) {
                queryHistoryInfo.setState(QueryHistoryInfo.HistoryState.SUCCESS);
                candidate.add(qh);
            } else {
                queryHistoryInfo.setState(QueryHistoryInfo.HistoryState.FAILED);
            }
            batchArgs.add(new Pair<>(qh.getId(), queryHistoryInfo));
        }
        return candidate;
    }

    private boolean matchInternalRule(QueryHistory queryHistory) {
        if (queryHistory.getQueryHistoryInfo() == null) {
            return false;
        }
        return InternalBlackOutRule.getSingletonInstance().filterCannotAccelerate(queryHistory);
    }

    public boolean matchCustomerRule(QueryHistory queryHistory, String project,
            Map<String, Set<String>> submitterToGroups) {
        var submitterRule = FavoriteRuleManager.getInstance(project)
                .getOrDefaultByName(FavoriteRule.SUBMITTER_RULE_NAME);
        boolean submitterMatch = matchRule(queryHistory, submitterRule,
                (queryHistory1, conditions) -> conditions.stream().anyMatch(cond -> queryHistory1.getQuerySubmitter()
                        .equals(((FavoriteRule.Condition) cond).getRightThreshold())));

        var groupRule = FavoriteRuleManager.getInstance(project)
                .getOrDefaultByName(FavoriteRule.SUBMITTER_GROUP_RULE_NAME);

        boolean userGroupMatch = matchRule(queryHistory, groupRule,
                (queryHistory1, conditions) -> conditions.stream()
                        .anyMatch(cond -> submitterToGroups
                                .computeIfAbsent(queryHistory1.getQuerySubmitter(), key -> new HashSet<>())
                                .contains(((FavoriteRule.Condition) cond).getRightThreshold())));

        var durationRule = FavoriteRuleManager.getInstance(project).getOrDefaultByName(FavoriteRule.DURATION_RULE_NAME);

        boolean durationMatch = matchRule(queryHistory, durationRule,
                (queryHistory1, conditions) -> conditions.stream().anyMatch(cond -> (queryHistory1
                        .getDuration() >= Long.parseLong(((FavoriteRule.Condition) cond).getLeftThreshold()) * 1000L
                        && queryHistory1.getDuration() <= Long
                                .parseLong(((FavoriteRule.Condition) cond).getRightThreshold()) * 1000L)));

        return (submitterMatch || userGroupMatch) && durationMatch;
    }

    private boolean matchRule(QueryHistory history, FavoriteRule favoriteRule,
            BiPredicate<QueryHistory, List<?>> function) {
        if ((FavoriteRule.SUBMITTER_RULE_NAME.equals(favoriteRule.getName())
                || FavoriteRule.SUBMITTER_GROUP_RULE_NAME.equals(favoriteRule.getName()))
                && !favoriteRule.isEnabled()) {
            return true;
        }

        return function.test(history, favoriteRule.getConds());
    }

}

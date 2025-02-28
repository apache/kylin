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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import lombok.val;

@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public class ModelFavoriteRuleManager extends BaseRuleManager {

    private final ModelFavoriteRuleStore favoriteRuleStore;
    private final String project;
    private final String modelId;

    private ModelFavoriteRuleManager(String project, String modelId) throws Exception {
        this.project = project;
        this.modelId = modelId;
        this.favoriteRuleStore = new ModelFavoriteRuleStore(KylinConfig.getInstanceFromEnv());
    }

    public static ModelFavoriteRuleManager getInstance(String project) {
        return Singletons.getInstance(project + "-null", ModelFavoriteRuleManager.class,
                clz -> new ModelFavoriteRuleManager(project, null));
    }

    public static ModelFavoriteRuleManager getInstance(String project, String modelId) {
        return Singletons.getInstance(project + "-" + modelId, ModelFavoriteRuleManager.class,
                clz -> new ModelFavoriteRuleManager(project, modelId));
    }

    public DataSourceTransactionManager getTransactionManager() {
        return favoriteRuleStore.getTransactionManager();
    }

    public List<FavoriteRule> getAllOfModel() {
        return favoriteRuleStore.queryByModel(modelId);
    }

    public List<FavoriteRule> getAllOfProject() {
        return favoriteRuleStore.queryByProject(project);
    }

    public List<FavoriteRule> listAll() {
        return FavoriteRule.FAVORITE_RULE_NAMES.stream().map(this::getOrDefaultByName).collect(Collectors.toList());
    }

    public FavoriteRule getByName(String name) {
        return favoriteRuleStore.queryByName(modelId, name);
    }

    public String getValue(String ruleName) {
        val rule = getOrDefaultByName(ruleName);
        FavoriteRule.Condition condition = (FavoriteRule.Condition) rule.getConds().get(0);
        return condition.getRightThreshold();
    }

    public FavoriteRule getOrDefaultByName(String ruleName) {
        return FavoriteRule.getDefaultRuleIfNull(getByName(ruleName), ruleName);
    }

    protected FavoriteRule copyForWrite(FavoriteRule rule) {
        // No need to copy, just return the origin object
        // This will be rewrite after metadata is refactored
        return rule;
    }

    public void updateRule(FavoriteRule rule) {
        updateRule(rule.getConds(), rule.isEnabled(), rule.getName());
    }

    public void updateRule(List<FavoriteRule.AbstractCondition> conditions, boolean isEnabled, String ruleName) {
        JdbcUtil.withTxAndRetry(getTransactionManager(), () -> {
            FavoriteRule copy = copyForWrite(getOrDefaultByName(ruleName));
            copy.setEnabled(isEnabled);
            List<FavoriteRule.AbstractCondition> newConditions = Lists.newArrayList();
            if (!conditions.isEmpty()) {
                newConditions.addAll(conditions);
            }
            copy.setConds(newConditions);
            saveOrUpdate(copy);
            return null;
        });
    }

    protected void saveOrUpdate(FavoriteRule rule) {
        if (rule.getId() == 0) {
            rule.setProject(project);
            rule.setModel(modelId);
            rule.setCreateTime(System.currentTimeMillis());
            rule.setUpdateTime(rule.getCreateTime());
            favoriteRuleStore.save(rule);
        } else {
            rule.setUpdateTime(System.currentTimeMillis());
            favoriteRuleStore.update(rule);
        }
    }

    public void delete(FavoriteRule favoriteRule) {
        favoriteRuleStore.deleteByName(modelId, favoriteRule.getName());
    }

    public void deleteByModel() {
        favoriteRuleStore.deleteByModel(modelId);
    }

    public void deleteByProject() {
        favoriteRuleStore.deleteByProject(project);
    }
}

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

import static org.apache.kylin.metadata.favorite.FavoriteRule.FAVORITE_RULE_NAMES;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.guava20.shaded.common.annotations.VisibleForTesting;
import lombok.val;

@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public class FavoriteRuleManager {

    private static final Logger logger = LoggerFactory.getLogger(FavoriteRuleManager.class);

    private final String project;

    private final KylinConfig kylinConfig;

    private CachedCrudAssist<FavoriteRule> crud;

    public static FavoriteRuleManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, FavoriteRuleManager.class);
    }

    // called by reflection
    static FavoriteRuleManager newInstance(KylinConfig config, String project) {
        return new FavoriteRuleManager(config, project);
    }

    private FavoriteRuleManager(KylinConfig kylinConfig, String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing FavoriteRuleManager with config {} for project {}", kylinConfig, project);

        this.kylinConfig = kylinConfig;
        this.project = project;
        init();
    }

    private void init() {

        final ResourceStore store = ResourceStore.getKylinMetaStore(this.kylinConfig);
        final String resourceRoot = "/" + this.project + ResourceStore.QUERY_FILTER_RULE_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<FavoriteRule>(store, resourceRoot, FavoriteRule.class) {
            @Override
            protected FavoriteRule initEntityAfterReload(FavoriteRule entity, String resourceName) {
                return entity;
            }
        };

        crud.setCheckCopyOnWrite(true);
        crud.reloadAll();
    }

    public List<FavoriteRule> getAll() {
        List<FavoriteRule> favoriteRules = Lists.newArrayList();

        favoriteRules.addAll(crud.listAll());
        return favoriteRules;
    }

    public List<FavoriteRule> listAll() {
        return FAVORITE_RULE_NAMES.stream().map(this::getOrDefaultByName).collect(Collectors.toList());
    }

    public FavoriteRule getByName(String name) {
        for (FavoriteRule rule : getAll()) {
            if (rule.getName().equals(name))
                return rule;
        }
        return null;
    }

    public String getValue(String ruleName) {
        val rule = getOrDefaultByName(ruleName);
        FavoriteRule.Condition condition = (FavoriteRule.Condition) rule.getConds().get(0);
        return condition.getRightThreshold();
    }

    public FavoriteRule getOrDefaultByName(String ruleName) {
        return FavoriteRule.getDefaultRuleIfNull(getByName(ruleName), ruleName);
    }

    public void resetRule() {
        FavoriteRule.getAllDefaultRule().forEach(this::updateRule);
    }

    public void updateRule(FavoriteRule rule) {
        updateRule(rule.getConds(), rule.isEnabled(), rule.getName());
    }

    public void updateRule(List<FavoriteRule.AbstractCondition> conditions, boolean isEnabled, String ruleName) {
        FavoriteRule copy = crud.copyForWrite(getOrDefaultByName(ruleName));

        copy.setEnabled(isEnabled);

        List<FavoriteRule.AbstractCondition> newConditions = Lists.newArrayList();
        if (!conditions.isEmpty()) {
            newConditions.addAll(conditions);
        }

        copy.setConds(newConditions);
        crud.save(copy);
    }

    public void delete(FavoriteRule favoriteRule) {
        crud.delete(favoriteRule);
    }

    @VisibleForTesting
    public void createRule(final FavoriteRule rule) {
        if (getByName(rule.getName()) != null)
            return;

        crud.save(rule);
    }

    @VisibleForTesting
    public List<FavoriteRule> getAllEnabled() {
        List<FavoriteRule> enabledRules = Lists.newArrayList();

        for (FavoriteRule rule : getAll()) {
            if (rule.isEnabled()) {
                enabledRules.add(rule);
            }
        }

        return enabledRules;
    }

    public Set<String> getExcludedTables() {
        FavoriteRule favoriteRule = getOrDefaultByName(FavoriteRule.EXCLUDED_TABLES_RULE);
        if (!favoriteRule.isEnabled()) {
            return Sets.newHashSet();
        }
        FavoriteRule.Condition condition = (FavoriteRule.Condition) favoriteRule.getConds().get(0);
        return Arrays.stream(condition.getRightThreshold().split(",")) //
                .map(table -> table.toUpperCase(Locale.ROOT)).collect(Collectors.toSet());
    }
}

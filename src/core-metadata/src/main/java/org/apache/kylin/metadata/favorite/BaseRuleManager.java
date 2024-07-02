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

import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

public abstract class BaseRuleManager {
    protected abstract DataSourceTransactionManager getTransactionManager();

    protected abstract FavoriteRule copyForWrite(FavoriteRule rule);

    protected abstract FavoriteRule getOrDefaultByName(String ruleName);

    protected abstract void saveOrUpdate(FavoriteRule rule);

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
}

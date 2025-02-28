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

package org.apache.kylin.rec.query.validator;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.rec.query.AbstractQueryRunner;
import org.apache.kylin.rec.query.SQLResult;
import org.apache.kylin.rec.query.advisor.ISqlAdvisor;
import org.apache.kylin.rec.query.advisor.SQLAdvice;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSQLValidator {

    @Getter
    protected final String project;
    protected KylinConfig kylinConfig;
    ISqlAdvisor sqlAdvisor;

    AbstractSQLValidator(String project, KylinConfig kylinConfig) {
        this.project = project;
        this.kylinConfig = kylinConfig;
    }

    abstract AbstractQueryRunner createQueryRunner(String[] sqls);

    public Map<String, SQLValidateResult> batchValidate(String[] sqls) {
        if (ArrayUtils.isEmpty(sqls)) {
            return Maps.newHashMap();
        }
        Map<String, SQLValidateResult> resultMap;
        try (AbstractQueryRunner queryRunner = createQueryRunner(sqls)) {
            try {
                queryRunner.execute();
            } catch (InterruptedException e) {
                log.warn("Interrupted!!", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("batch validate sql error" + Arrays.toString(sqls), e);
            }
            resultMap = advice(queryRunner.getQueryResults());
        }
        return resultMap;
    }

    private Map<String, SQLValidateResult> advice(Map<String, SQLResult> queryResultMap) {

        Map<String, SQLValidateResult> validateStatsMap = Maps.newHashMap();
        queryResultMap.forEach((sql, sqlResult) -> {
            SQLAdvice advice = sqlAdvisor.propose(sqlResult);
            SQLValidateResult result = Objects.isNull(advice) //
                    ? SQLValidateResult.successStats(sqlResult)
                    : SQLValidateResult.failedStats(Lists.newArrayList(advice), sqlResult);
            validateStatsMap.put(sql, result);
        });
        return validateStatsMap;
    }
}

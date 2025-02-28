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

package org.apache.kylin.rec;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.model.GreedyModelTreesBuilder;
import org.apache.kylin.rec.query.AbstractQueryRunner;
import org.apache.kylin.rec.query.QueryRunnerBuilder;
import org.apache.kylin.rec.query.SQLResult;
import org.apache.kylin.rec.query.advisor.SQLAdvice;
import org.apache.kylin.rec.query.advisor.SqlSyntaxAdvisor;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SQLAnalysisProposer extends AbstractProposer {

    private final String[] sqls;

    public SQLAnalysisProposer(AbstractContext proposeContext) {
        super(proposeContext);
        this.sqls = Objects.requireNonNull(proposeContext.getSqlArray());
    }

    @Override
    public void execute() {
        initAccelerationInfo(sqls);
        List<NDataModel> models = proposeContext.getOriginModels();
        try (AbstractQueryRunner extractor = new QueryRunnerBuilder(project,
                getProposeContext().getSmartConfig().getKylinConfig(), sqls).of(models).build()) {
            extractor.execute();
            logFailedQuery(extractor);

            val modelContexts = new GreedyModelTreesBuilder(KylinConfig.getInstanceFromEnv(), project, proposeContext)
                    .build(extractor.filterNonModelViewOlapContexts(), null) //
                    .stream() //
                    .filter(modelTree -> !modelTree.getOlapContexts().isEmpty()) //
                    .map(proposeContext::createModelContext) //
                    .collect(Collectors.toList());
            proposeContext.getModelViewOlapContextMap().putAll(extractor.filterModelViewOlapContexts());
            proposeContext.setModelContexts(modelContexts);
        } catch (InterruptedException e) {
            log.warn("Interrupted!!", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Failed to get query stats. ", e);
        }
    }

    /**
     * Init acceleration info
     */
    private void initAccelerationInfo(String[] sqls) {
        Arrays.stream(sqls).forEach(sql -> {
            AccelerateInfo accelerateInfo = new AccelerateInfo();
            if (!proposeContext.getAccelerateInfoMap().containsKey(sql)) {
                proposeContext.getAccelerateInfoMap().put(sql, accelerateInfo);
            }
        });
    }

    private void logFailedQuery(AbstractQueryRunner extractor) {
        final Map<String, SQLResult> queryResultMap = extractor.getQueryResults();
        SqlSyntaxAdvisor sqlAdvisor = new SqlSyntaxAdvisor();

        queryResultMap.forEach((sql, sqlResult) -> {
            if (sqlResult.getStatus() != SQLResult.Status.FAILED) {
                return;
            }
            AccelerateInfo accelerateInfo = proposeContext.getAccelerateInfoMap().get(sql);
            Preconditions.checkNotNull(accelerateInfo);
            Throwable throwable = sqlResult.getException();
            if (!(throwable instanceof NoRealizationFoundException
                    || throwable.getCause() instanceof NoRealizationFoundException)) {
                if (StringUtils.contains(throwable.getMessage(), "not found")) {
                    SQLAdvice sqlAdvices = sqlAdvisor.proposeWithMessage(sqlResult);
                    accelerateInfo.setPendingMsg(sqlAdvices.getIncapableReason());
                } else {
                    accelerateInfo.setFailedCause(AccelerateInfo.transformThrowable(throwable));
                }
            }
        });
    }

    @Override
    public String getIdentifierName() {
        return "SQLAnalysisProposer";
    }
}

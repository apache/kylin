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

package org.apache.kylin.query.runtime;

import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.rel.RelNode;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryTrace;
import org.apache.kylin.common.exception.DryRunSucceedException;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.query.engine.exec.ExecuteResult;
import org.apache.kylin.query.engine.exec.sparder.QueryEngine;
import org.apache.kylin.query.mask.QueryResultMasks;
import org.apache.kylin.query.runtime.plan.ResultPlan;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkInternalAgent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkEngine implements QueryEngine {
    private static final Logger log = LoggerFactory.getLogger(SparkEngine.class);

    private Dataset<Row> toSparkPlan(DataContext dataContext, RelNode relNode) {
        QueryContext.currentTrace().startSpan(QueryTrace.PREPARE_AND_SUBMIT_JOB);
        log.info("Begin planning spark plan.");
        long start = System.currentTimeMillis();
        CalciteToSparkPlaner calciteToSparkPlaner = new CalciteToSparkPlaner(dataContext);
        try {
            calciteToSparkPlaner.go(relNode);
        } finally {
            calciteToSparkPlaner.cleanCache();
        }
        LogicalPlan plan = calciteToSparkPlaner.getResult();
        Dataset<Row> df = SparkInternalAgent.getDataFrame(SparderEnv.getSparkSession(), plan);
        QueryContext.current().record("to_spark_plan");
        long takeTime = System.currentTimeMillis() - start;
        log.info("Plan take {} ms", takeTime);
        return df;
    }

    @Override
    public List<List<String>> compute(DataContext dataContext, RelNode relNode) {
        return ImmutableList.copyOf(computeToIterable(dataContext, relNode).getRows());
    }

    @Override
    public ExecuteResult computeToIterable(DataContext dataContext, RelNode relNode) {
        Dataset<Row> sparkPlan = QueryResultMasks.maskResult(toSparkPlan(dataContext, relNode));
        if (QueryContext.current().isDryRun()) {
            throw new DryRunSucceedException("DryRun succeed. Query is stopped due to DryRun enabled.",
                    sparkPlan.queryExecution().executedPlan().toString());
        }
        LogicalPlan logicalPlan = sparkPlan.queryExecution().logical();
        QueryContext.current().getQueryPlan().setSparkPlan(logicalPlan.toString());
        log.info("SPARK LOGICAL PLAN {}", logicalPlan);
        if (QueryContext.current().isExplainSql()) {
            return new ExecuteResult(Lists.newArrayList(), 0);
        } else if (KapConfig.getInstanceFromEnv().isOnlyPlanInSparkEngine()) {
            return ResultPlan.completeResultForMdx(sparkPlan, relNode.getRowType());
        } else {
            return ResultPlan.getResult(sparkPlan, relNode.getRowType());
        }
    }
}

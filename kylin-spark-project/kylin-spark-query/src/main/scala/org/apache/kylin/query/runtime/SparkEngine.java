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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.common.QueryTrace;
import org.apache.kylin.query.exec.QueryEngine;
import org.apache.kylin.query.runtime.plans.ResultPlan;
import org.apache.kylin.query.runtime.plans.ResultType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SparkEngine implements QueryEngine {
    private static final Logger log = LoggerFactory.getLogger(SparkEngine.class);

    @Override
    public Enumerable<Object> computeSCALA(DataContext dataContext, RelNode relNode, RelDataType resultType) {
        Dataset<Row> sparkPlan = toSparkPlan(dataContext, relNode);
        if (System.getProperty("calcite.debug") != null) {
            log.debug("SPARK LOGICAL PLAN {}", sparkPlan.queryExecution());
        }
        return ResultPlan.getResult(sparkPlan, resultType, ResultType.SCALA()).right().get();

    }

    @Override
    public Enumerable<Object[]> compute(DataContext dataContext, RelNode relNode, RelDataType resultType) {
        Dataset<Row> sparkPlan = toSparkPlan(dataContext, relNode);
        if (System.getProperty("calcite.debug") != null) {
            log.info("SPARK LOGICAL PLAN {}", sparkPlan.queryExecution());
        }
        return ResultPlan.getResult(sparkPlan, resultType, ResultType.NORMAL()).left().get();
    }

    private Dataset<Row> toSparkPlan(DataContext dataContext, RelNode relNode) {
        log.trace("Begin planning spark plan.");
        QueryContextFacade.current().getQueryTrace().startSpan(QueryTrace.PREPARE_AND_SUBMIT_JOB);
        long start = System.currentTimeMillis();
        CalciteToSparkPlaner calciteToSparkPlaner = new CalciteToSparkPlaner(dataContext);
        calciteToSparkPlaner.go(relNode);
        long takeTime = System.currentTimeMillis() - start;
        log.trace("Plan take {} ms", takeTime);
        return calciteToSparkPlaner.getResult();
    }
}

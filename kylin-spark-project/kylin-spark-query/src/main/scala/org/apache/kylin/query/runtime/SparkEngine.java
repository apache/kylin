/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.kylin.query.runtime;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
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
        log.debug("SPARK LOGICAL PLAN {}", sparkPlan.queryExecution().logical());
        return ResultPlan.getResult(sparkPlan, resultType, ResultType.SCALA()).right().get();

    }

    @Override
    public Enumerable<Object[]> compute(DataContext dataContext, RelNode relNode, RelDataType resultType) {
        Dataset<Row> sparkPlan = toSparkPlan(dataContext, relNode);
        log.debug("SPARK LOGICAL PLAN {}", sparkPlan.queryExecution().logical());
        return ResultPlan.getResult(sparkPlan, resultType, ResultType.NORMAL()).left().get();
    }

    private Dataset<Row> toSparkPlan(DataContext dataContext, RelNode relNode) {
        log.info("Begin planning spark plan.");
        long start = System.currentTimeMillis();
        CalciteToSparkPlaner calciteToSparkPlaner = new CalciteToSparkPlaner(dataContext);
        long t = System.currentTimeMillis();
        calciteToSparkPlaner.go(relNode);
        long takeTime = System.currentTimeMillis() - start;
        log.info("Plan take {} ms", takeTime);
        return calciteToSparkPlaner.getResult();
    }
}

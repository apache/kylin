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

package org.apache.kylin.query.util;

import java.sql.SQLException;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.relnode.OlapAggregateRel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;

import lombok.val;

public class QueryHelper {

    private QueryHelper() {
    }

    static final String RUN_CONSTANT_QUERY_LOCALLY = "kylin.query.engine.run-constant-query-locally";

    // Expose this method for MDX, don't remove it.
    public static Dataset<Row> singleQuery(String sql, String project) throws SQLException {
        val prevRunLocalConf = Unsafe.setProperty(RUN_CONSTANT_QUERY_LOCALLY, "FALSE");
        try {
            val projectKylinConfig = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project)
                    .getConfig();
            val queryExec = new QueryExec(project, projectKylinConfig);
            val queryParams = new QueryParams(NProjectManager.getProjectConfig(project), sql, project, 0, 0,
                    queryExec.getDefaultSchemaName(), true);
            val convertedSql = QueryUtil.massageSql(queryParams);
            queryExec.executeQuery(convertedSql);
        } finally {
            if (prevRunLocalConf == null) {
                Unsafe.clearProperty(RUN_CONSTANT_QUERY_LOCALLY);
            } else {
                Unsafe.setProperty(RUN_CONSTANT_QUERY_LOCALLY, prevRunLocalConf);
            }
        }
        return SparderEnv.getDF();
    }

    public static Dataset<Row> sql(SparkSession session, String project, String sqlText) {
        try {
            return singleQuery(sqlText, project);
        } catch (SQLException e) {
            return session.sql(sqlText);
        }
    }

    public static boolean isConstantQueryAndCalciteEngineCapable(RelNode rel) {
        return isConstantQuery(rel) && isCalciteEngineCapable(rel);
    }

    /**
     * search rel node tree to see if there is any table scan node
     * @param rel
     * @return
     */
    public static boolean isConstantQuery(RelNode rel) {
        if (TableScan.class.isAssignableFrom(rel.getClass())) {
            return false;
        }
        for (RelNode input : rel.getInputs()) {
            if (!isConstantQuery(input)) {
                return false;
            }
        }

        return true;
    }

    public static boolean isCalciteEngineCapable(RelNode rel) {
        if (rel instanceof Project) {
            Project projectRelNode = (Project) rel;
            if (projectRelNode.getProjects().stream().filter(RexCall.class::isInstance)
                    .anyMatch(pRelNode -> pRelNode.accept(new CalcitePlanRouterVisitor()))) {
                return false;
            }
            if (projectRelNode.getProjects() != null
                    && projectRelNode.getProjects().stream().anyMatch(QueryHelper::isPlusString)) {
                return false;
            }
        }

        if (rel instanceof OlapAggregateRel) {
            OlapAggregateRel aggregateRel = (OlapAggregateRel) rel;
            if (aggregateRel.getAggCallList().stream().anyMatch(
                    aggCall -> FunctionDesc.FUNC_BITMAP_BUILD.equalsIgnoreCase(aggCall.getAggregation().getName()))) {
                return false;
            }
            if (aggregateRel.getAggCallList().stream().anyMatch(AggregateCall::isDistinct)) {
                return false;
            }
        }

        return rel.getInputs().stream().allMatch(QueryHelper::isCalciteEngineCapable);
    }

    private static boolean isPlusString(RexNode node) {
        if (node instanceof RexCall) {
            RexCall rexCall = (RexCall) node;
            if ("plus".equals(rexCall.getOperator().getKind().lowerName)) {
                for (RexNode operand : rexCall.operands) {
                    if (operand.getType().getFamily() == SqlTypeFamily.STRING
                            || operand.getType().getFamily() == SqlTypeFamily.CHARACTER) {
                        return true;
                    }
                }
            }
            return rexCall.getOperands().stream().anyMatch(QueryHelper::isPlusString);
        }
        return false;
    }
}

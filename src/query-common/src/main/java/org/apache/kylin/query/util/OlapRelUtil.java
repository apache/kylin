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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Function;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.query.QueryExtension;
import org.apache.kylin.query.engine.KylinConnectionConfig;
import org.apache.kylin.query.engine.SqlConverter;
import org.apache.kylin.query.engine.UdfRegistry;
import org.apache.kylin.query.schema.OlapSchema;

public class OlapRelUtil {

    private static final String CTX = "ctx=";

    private OlapRelUtil() {
    }

    public static RelNode toRel(String project, KylinConfig kylinConfig, String sql) throws SqlParseException {
        KylinConnectionConfig connectionConfig = KylinConnectionConfig.fromKapConfig(kylinConfig);
        CalciteSchema rootSchema = createRootSchema(project, kylinConfig);
        String defaultSchemaName = "DEFAULT";
        Prepare.CatalogReader catalogReader = SqlConverter.createCatalogReader(connectionConfig, rootSchema,
                defaultSchemaName);

        Collection<RelOptRule> optRules = new LinkedHashSet<>();
        optRules.add(CoreRules.PROJECT_REDUCE_EXPRESSIONS);
        // optRules.add(CoreRules.CALC_REDUCE_EXPRESSIONS)
        HepProgram program = HepProgram.builder().addRuleCollection(optRules).build();
        HepPlanner planner = new HepPlanner(program, null, true, null, RelOptCostImpl.FACTORY);
        SqlConverter sqlConverter = new SqlConverter(connectionConfig, planner, catalogReader);
        RelRoot relRoot = sqlConverter.convertSqlToRelNode(sql);
        // RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(relRoot.rel.getCluster(), null)
        // RelNode node = new RelFieldTrimmer(null, relBuilder).trim(relRoot.rel)
        planner.setRoot(relRoot.rel);
        return planner.findBestExp();
    }

    private static CalciteSchema createRootSchema(String project, KylinConfig kylinConfig) {
        Map<String, List<TableDesc>> schemasMap = QueryExtension.getFactory().getSchemaMapExtension()
                .getAuthorizedTablesAndColumns(kylinConfig, project, true, null, null);
        Map<String, List<NDataModel>> modelsMap = NDataflowManager.getInstance(kylinConfig, project)
                .getModelsGroupbyTable();
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        schemasMap.forEach((schemaName, list) -> {
            OlapSchema olapSchema = new OlapSchema(kylinConfig, project, schemaName, schemasMap.get(schemaName),
                    modelsMap);
            CalciteSchema schema = rootSchema.add(schemaName, olapSchema);
            for (Map.Entry<String, Function> entry : UdfRegistry.allUdfMap.entries()) {
                schema.plus().add(entry.getKey().toUpperCase(Locale.ROOT), entry.getValue());
            }
        });
        return rootSchema;
    }

    public static String replaceDigestCtxValueByLayoutIdAndModelId(String digestId, long layoutId, String modelId) {
        if (layoutId <= 0 || "".equals(modelId)) {
            return digestId;
        }
        StringBuilder digestBuilder = new StringBuilder();
        char[] digestArray = digestId.toCharArray();
        char[] compareArray = CTX.toCharArray();
        int len = digestArray.length;
        for (int i = 0; i < len; i++) {
            char c1 = digestArray[i];
            if (c1 == compareArray[0] && (i + 3) < len && digestArray[i + 1] == compareArray[1]
                    && digestArray[i + 2] == compareArray[2] && digestArray[i + 3] == compareArray[3]) {
                digestBuilder.append(CTX);
                i = i + 2;
                while (digestArray[i + 1] != ',' && digestArray[i + 1] != ')') {
                    i = i + 1;
                }
                digestBuilder.append(modelId).append("_").append(layoutId);
            } else {
                digestBuilder.append(c1);
            }
        }
        return digestBuilder.toString();
    }

    public static RexNode isNotDistinctFrom(RelNode left, RelNode right, RexNode condition,
            List<Pair<Integer, Integer>> pairs, List<Boolean> filterNulls) {
        final List<Integer> leftKeys = new ArrayList<>();
        final List<Integer> rightKeys = new ArrayList<>();
        RexNode rexNode = RelOptUtil.splitJoinCondition(left, right, condition, leftKeys, rightKeys, filterNulls);
        for (int i = 0; i < leftKeys.size(); i++) {
            pairs.add(new Pair<>(leftKeys.get(i), rightKeys.get(i) + left.getRowType().getFieldCount()));
        }
        return rexNode;
    }
}

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CollectionUtil;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.alias.ExpressionComparator;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.relnode.KapAggregateRel;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.TableColRefWithRel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.val;
import lombok.var;

public class ComputedColumnRewriter {

    private static final Logger logger = LoggerFactory.getLogger(ComputedColumnRewriter.class);

    private ComputedColumnRewriter() {
    }

    public static void rewriteCcInnerCol(OLAPContext context, NDataModel model, QueryAliasMatchInfo matchInfo) {
        KylinConfig projectConfig = NProjectManager.getProjectConfig(model.getProject());
        rewriteAggInnerCol(projectConfig, context, model, matchInfo);
        rewriteTopNInnerCol(projectConfig, context, model, matchInfo);
        rewriteGroupByInnerCol(projectConfig, context, model, matchInfo);

        // rewrite inner column of filter is not support yet.
        // rewriteFilterInnerCol(projectConfig, context, model, matchInfo)
    }

    private static void rewriteAggInnerCol(KylinConfig kylinConfig, OLAPContext context, NDataModel model,
            QueryAliasMatchInfo matchInfo) {
        if (!KapConfig.getInstanceFromEnv().isAggComputedColumnRewriteEnabled()
                || CollectionUtils.isEmpty(model.getComputedColumnDescs())) {
            return;
        }

        context.aggregations.stream().filter(agg -> CollectionUtils.isNotEmpty(agg.getParameters())).forEach(agg -> {
            List<ParameterDesc> parameters = Lists.newArrayList();
            for (ParameterDesc parameter : agg.getParameters()) {
                if (!parameter.getColRef().isInnerColumn()) {
                    parameters.add(parameter);
                    continue;
                }

                TblColRef translatedInnerCol = rewriteInnerColumnToTblColRef(kylinConfig,
                        parameter.getColRef().getParserDescription(), model, matchInfo);
                if (translatedInnerCol != null) {
                    parameters.add(ParameterDesc.newInstance(translatedInnerCol));
                    context.allColumns.add(translatedInnerCol);
                }
            }

            if (!parameters.isEmpty()) {
                agg.setParameters(parameters);
            }
        });
    }

    private static TblColRef rewriteInnerColumnToTblColRef(KylinConfig kylinConfig, String innerExpression,
            NDataModel model, QueryAliasMatchInfo matchInfo) {
        SqlNode innerColExpr;
        try {
            innerColExpr = CalciteParser.getExpNode(innerExpression);
        } catch (IllegalStateException e) {
            logger.warn("Failed to resolve CC expr for {}", innerExpression, e);
            return null;
        }

        for (ComputedColumnDesc cc : model.getComputedColumnDescs()) {
            if (kylinConfig.isTableExclusionEnabled() && kylinConfig.onlyReuseUserDefinedCC() && cc.isAutoCC()) {
                continue;
            }

            SqlNode ccExpressionNode = CalciteParser.getExpNode(cc.getExpression());
            if (ExpressionComparator.isNodeEqual(innerColExpr, ccExpressionNode, matchInfo,
                    new AliasDeduceImpl(matchInfo))) {
                var ccCols = ComputedColumnUtil.createComputedColumns(Lists.newArrayList(cc),
                        model.getRootFactTable().getTableDesc());
                logger.info("Replacing CC expr [{},{}]", cc.getColumnName(), cc.getExpression());
                return new TblColRef(model.getRootFactTable(), ccCols[0]);
            }
        }

        return null;
    }

    private static void rewriteTopNInnerCol(KylinConfig kylinConfig, OLAPContext context, NDataModel model,
            QueryAliasMatchInfo matchInfo) {
        if (CollectionUtils.isEmpty(model.getComputedColumnDescs()))
            return;

        context.getSortColumns().stream().filter(TblColRef::isInnerColumn).forEach(column -> {
            if (CollectionUtils.isEmpty(column.getOperands()))
                return;

            val translatedOperands = Lists.<TblColRef> newArrayList();
            for (TblColRef tblColRef : column.getOperands()) {
                val innerExpression = tblColRef.getParserDescription();
                if (innerExpression == null)
                    continue;

                val translatedInnerCol = rewriteInnerColumnToTblColRef(kylinConfig, innerExpression, model, matchInfo);
                if (translatedInnerCol != null)
                    translatedOperands.add(translatedInnerCol);
            }

            column.setOperands(translatedOperands);
        });
    }

    private static void rewriteGroupByInnerCol(KylinConfig kylinConfig, OLAPContext ctx, NDataModel model,
            QueryAliasMatchInfo matchInfo) {
        if (CollectionUtils.isEmpty(model.getComputedColumnDescs())) {
            return;
        }

        // collect all aggRel with candidate CC group keys
        Map<KapAggregateRel, Map<TblColRef, TblColRef>> relColReplacementMapping = new HashMap<>();
        for (TableColRefWithRel tableColRefWIthRel : ctx.getInnerGroupByColumns()) {
            SqlNode innerColExpr;
            try {
                innerColExpr = CalciteParser.getExpNode(tableColRefWIthRel.getTblColRef().getParserDescription());
            } catch (IllegalStateException e) {
                logger.warn("Failed to resolve CC expr for {}",
                        tableColRefWIthRel.getTblColRef().getParserDescription(), e);
                continue;
            }
            for (ComputedColumnDesc cc : model.getComputedColumnDescs()) {
                if (kylinConfig.isTableExclusionEnabled() && kylinConfig.onlyReuseUserDefinedCC() && cc.isAutoCC()) {
                    continue;
                }
                SqlNode ccExpressionNode = CalciteParser.getExpNode(cc.getExpression());
                if (ExpressionComparator.isNodeEqual(innerColExpr, ccExpressionNode, matchInfo,
                        new AliasDeduceImpl(matchInfo))) {
                    var ccCols = ComputedColumnUtil.createComputedColumns(Lists.newArrayList(cc),
                            model.getRootFactTable().getTableDesc());

                    CollectionUtil.find(ctx.firstTableScan.getColumnRowType().getAllColumns(),
                            colRef -> colRef.getColumnDesc().equals(ccCols[0])).ifPresent(ccColRef -> {
                                relColReplacementMapping.putIfAbsent(
                                        tableColRefWIthRel.getRelNodeAs(KapAggregateRel.class), new HashMap<>());
                                relColReplacementMapping.get(tableColRefWIthRel.getRelNodeAs(KapAggregateRel.class))
                                        .put(tableColRefWIthRel.getTblColRef(), ccColRef);
                                logger.info("Replacing CC expr [{},{}] in group key {}", cc.getColumnName(),
                                        cc.getExpression(), tableColRefWIthRel.getTblColRef());
                            });
                }
            }
        }

        // rebuild aggRel group keys with CC cols
        for (Map.Entry<KapAggregateRel, Map<TblColRef, TblColRef>> kapAggregateRelMapEntry : relColReplacementMapping
                .entrySet()) {
            KapAggregateRel aggRel = kapAggregateRelMapEntry.getKey();
            Map<TblColRef, TblColRef> colReplacementMapping = kapAggregateRelMapEntry.getValue();
            aggRel.reBuildGroups(colReplacementMapping);
        }
    }
}

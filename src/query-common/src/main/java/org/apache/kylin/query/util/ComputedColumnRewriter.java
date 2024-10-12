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
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CollectionUtil;
import org.apache.kylin.common.util.StringHelper;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.relnode.OlapAggregateRel;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.relnode.TableColRefWithRel;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ComputedColumnRewriter {

    // use concurrent map for multi-thread
    public static final Map<String, String> CC_TO_REX_NODE_STR_CACHE = Maps.newConcurrentMap();
    public static final Map<String, ComputedColumnDesc> EXP_TO_CC_MAP = Maps.newConcurrentMap();
    public static final String NAME_SPLIT = "@@";
    private static final SparkSQLFunctionConverter FUNCTION_CONVERTER = new SparkSQLFunctionConverter();

    private ComputedColumnRewriter() {
    }

    public static void rewriteCcInnerCol(OlapContext context, NDataModel model, QueryAliasMatchInfo matchInfo) {
        KylinConfig projectConfig = NProjectManager.getProjectConfig(model.getProject());
        if (canSkipRewrite(projectConfig, model)) {
            return;
        }
        rewriteAggInnerCol(projectConfig, context, model, matchInfo);
        rewriteTopNInnerCol(projectConfig, context, model, matchInfo);
        rewriteGroupByInnerCol(projectConfig, context, model, matchInfo);
    }

    private static void rewriteAggInnerCol(KylinConfig kylinConfig, OlapContext context, NDataModel model,
            QueryAliasMatchInfo matchInfo) {
        for (FunctionDesc agg : context.getAggregations()) {
            if (CollectionUtils.isEmpty(agg.getParameters())) {
                continue;
            }
            List<ParameterDesc> parameters = Lists.newArrayList();
            for (ParameterDesc parameter : agg.getParameters()) {
                if (!parameter.getColRef().isInnerColumn()) {
                    parameters.add(parameter);
                    continue;
                }
                String innerExp = parameter.getColRef().getParserDescription();
                TblColRef translatedInnerCol = rewriteInnerCol(kylinConfig, innerExp, model, matchInfo);
                if (translatedInnerCol != null) {
                    parameters.add(ParameterDesc.newInstance(translatedInnerCol));
                    context.getAllColumns().add(translatedInnerCol);
                }
            }
            if (!parameters.isEmpty()) {
                agg.setParameters(parameters);
            }
        }
    }

    private static TblColRef rewriteInnerCol(KylinConfig kylinConfig, String innerExpression, NDataModel model,
            QueryAliasMatchInfo matchInfo) {
        try {
            ComputedColumnDesc cc = matchComputedColumn(kylinConfig, innerExpression, model, matchInfo);
            if (cc != null) {
                ColumnDesc[] ccCols = ComputedColumnUtil.createComputedColumns(Lists.newArrayList(cc),
                        model.getRootFactTable().getTableDesc());
                log.debug("Replacing CC expr [{},{}]", cc.getColumnName(), cc.getExpression());
                return new TblColRef(model.getRootFactTable(), ccCols[0]);
            }
        } catch (Exception e) {
            // ignore the exception
            log.warn("Failed to parse expression: `{}`, therefore cannot match ComputedColumn.", innerExpression);
        }
        return null;
    }

    /**
     * Match the innerExpression with RexNode.
     */
    private static ComputedColumnDesc matchComputedColumn(KylinConfig kylinConfig, String innerExpression,
            NDataModel model, QueryAliasMatchInfo matchInfo) throws SqlParseException {
        if (innerExpression == null) {
            return null;
        }

        // update cc cache first, because the cc may have been updated
        for (ComputedColumnDesc cc : model.getComputedColumnDescs()) {
            String key = cc.getTableIdentity() + NAME_SPLIT
                    + StringHelper.backtickToDoubleQuote(cc.getInnerExpression());
            EXP_TO_CC_MAP.put(key, cc);
            if (!CC_TO_REX_NODE_STR_CACHE.containsKey(key)) {
                String rexNodeStr = extractCcRexNode(model, kylinConfig, cc.getInnerExpression());
                CC_TO_REX_NODE_STR_CACHE.put(key, rexNodeStr);
            }
        }

        // replace table alias of innerExpression, then search cache
        for (Map.Entry<String, String> entry : matchInfo.getAliasMap().entrySet()) {
            innerExpression = innerExpression.replace(entry.getKey(), entry.getValue());
        }
        boolean onlyReuseUserDefinedCC = kylinConfig.onlyReuseUserDefinedCC();
        String aliasInnerExpression = model.getRootFactTableName() + NAME_SPLIT + innerExpression;
        if (EXP_TO_CC_MAP.containsKey(aliasInnerExpression)) {
            ComputedColumnDesc cc = EXP_TO_CC_MAP.get(aliasInnerExpression);
            if (onlyReuseUserDefinedCC && cc.isAutoCC()) {
                return null;
            }
            return cc;
        }

        // search by rexNode String
        String rexNodeStr = extractCcRexNode(model, kylinConfig, innerExpression);
        for (ComputedColumnDesc cc : model.getComputedColumnDescs()) {
            if (onlyReuseUserDefinedCC && cc.isAutoCC()) {
                continue;
            }
            String key = cc.getTableIdentity() + NAME_SPLIT
                    + StringHelper.backtickToDoubleQuote(cc.getInnerExpression());
            String rexNodeStrOfCc = CC_TO_REX_NODE_STR_CACHE.get(key);
            if (Objects.equals(rexNodeStr, rexNodeStrOfCc)) {
                return cc;
            }
        }
        return null;
    }

    private static String extendToQuery(NDataModel model, String project, String expression) {
        if (StringUtils.isBlank(expression)) {
            return null;
        }
        String ccSql = PushDownUtil.expandComputedColumnExp(model, project,
                StringHelper.backtickToDoubleQuote(expression));
        ccSql = FUNCTION_CONVERTER.convert(ccSql, project, PushDownUtil.DEFAULT_SCHEMA);
        return QueryUtil.adaptCalciteSyntax(ccSql);
    }

    public static String extractCcRexNode(NDataModel model, KylinConfig kylinConfig, String innerExpression)
            throws SqlParseException {
        String project = model.getProject();
        String extendedQuery = extendToQuery(model, project, innerExpression);
        return getRexNodeStr(kylinConfig, project, extendedQuery);
    }

    public static String getRexNodeStr(KylinConfig kylinConfig, String project, String extendedQuery)
            throws SqlParseException {
        RelNode relNode = OlapRelUtil.toRel(project, kylinConfig, extendedQuery);
        if (relNode instanceof LogicalProject) {
            List<RexNode> projects = ((LogicalProject) relNode).getProjects();
            RexNode rexNode = projects.get(0);
            InputRefFinder finder = new InputRefFinder(((LogicalProject) relNode).getInput());
            RexNode accepted = rexNode.accept(finder);
            accepted = RexUtils.symmetricalExchange(relNode.getCluster().getRexBuilder(), accepted);
            String str = accepted.toString();
            for (Map.Entry<String, RelDataTypeField> entry : finder.getFieldMap().entrySet()) {
                str = str.replace(entry.getKey(), entry.getValue().getName());
            }
            return str;
        }
        return null;
    }

    public static class InputRefFinder extends RexVisitorImpl<RexNode> {
        @Getter
        private Map<String, RelDataTypeField> fieldMap = Maps.newHashMap();

        final RelNode sourceRel;

        public InputRefFinder(RelNode sourceRel) {
            super(true);
            this.sourceRel = sourceRel;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            RelDataTypeField field = sourceRel.getRowType().getFieldList().get(inputRef.getIndex());
            fieldMap.put("$" + inputRef.getIndex(), field);
            return inputRef;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            List<RexNode> rexNodes = call.getOperands();
            List<RexNode> converted = rexNodes.stream() //
                    .map(rex -> rex.accept(this)) //
                    .collect(Collectors.toList());
            if (call.getOperator().getName().equalsIgnoreCase("substr")) {
                return sourceRel.getCluster().getRexBuilder() //
                        .makeCall(call.getType(), SqlStdOperatorTable.SUBSTRING, converted);
            }
            return call.clone(call.getType(), converted);
        }

        @Override
        public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
            return dynamicParam;
        }

        @Override
        public RexNode visitLiteral(RexLiteral literal) {
            return literal;
        }
    }

    private static void rewriteTopNInnerCol(KylinConfig kylinConfig, OlapContext context, NDataModel model,
            QueryAliasMatchInfo matchInfo) {
        context.getSortColumns().stream().filter(TblColRef::isInnerColumn).forEach(column -> {
            if (CollectionUtils.isEmpty(column.getOperands()))
                return;

            List<TblColRef> translatedOperands = Lists.newArrayList();
            for (TblColRef tblColRef : column.getOperands()) {
                String innerExp = tblColRef.getParserDescription();
                if (innerExp == null)
                    continue;

                TblColRef translated = rewriteInnerCol(kylinConfig, innerExp, model, matchInfo);
                if (translated != null)
                    translatedOperands.add(translated);
            }

            column.setOperands(translatedOperands);
        });
    }

    private static void rewriteGroupByInnerCol(KylinConfig kylinConfig, OlapContext context, NDataModel model,
            QueryAliasMatchInfo matchInfo) {
        // collect all aggRel with candidate CC group keys
        Map<OlapAggregateRel, Map<TblColRef, TblColRef>> relColRefMapping = new HashMap<>();
        for (TableColRefWithRel innerColRefWithRel : context.getInnerGroupByColumns()) {
            String innerExp = innerColRefWithRel.getTblColRef().getParserDescription();
            TblColRef translated = rewriteInnerCol(kylinConfig, innerExp, model, matchInfo);
            if (translated == null) {
                continue;
            }
            ColumnDesc innerCol = translated.getColumnDesc();
            CollectionUtil.find(context.getFirstTableScan().getColumnRowType().getAllColumns(),
                    tblColRef -> tblColRef.getColumnDesc().equals(innerCol)).ifPresent(ccColRef -> {
                        OlapAggregateRel olapAggRel = innerColRefWithRel.getRelNodeAs(OlapAggregateRel.class);
                        relColRefMapping.putIfAbsent(olapAggRel, new HashMap<>());
                        relColRefMapping.get(olapAggRel).put(innerColRefWithRel.getTblColRef(), ccColRef);
                        log.info("Replacing CC expr [{},{}] in group key {}", innerCol.getName(),
                                innerCol.getComputedColumnExpr(), innerColRefWithRel.getTblColRef());
                    });
        }

        // rebuild aggRel group keys with CC cols
        relColRefMapping.forEach(OlapAggregateRel::reBuildGroups);
    }

    private static boolean canSkipRewrite(KylinConfig kylinConfig, NDataModel model) {
        return CollectionUtils.isEmpty(model.getComputedColumnDescs()) || !kylinConfig.isConvertExpressionToCcEnabled();
    }

    public static void cacheCcRexNode(KylinConfig kylinConfig, String project) {
        NDataflowManager dflMgr = NDataflowManager.getInstance(kylinConfig, project);
        boolean onlyReuseUserDefinedCC = kylinConfig.onlyReuseUserDefinedCC();
        dflMgr.listOnlineDataModels().forEach(model -> {
            for (ComputedColumnDesc cc : model.getComputedColumnDescs()) {
                if (onlyReuseUserDefinedCC && cc.isAutoCC()) {
                    continue;
                }
                try {
                    String rexNodeStr;
                    String key = cc.getTableIdentity() + NAME_SPLIT
                            + StringHelper.backtickToDoubleQuote(cc.getInnerExpression());
                    if (!ComputedColumnRewriter.CC_TO_REX_NODE_STR_CACHE.containsKey(key)) {
                        rexNodeStr = ComputedColumnRewriter.extractCcRexNode(model, kylinConfig,
                                cc.getInnerExpression());
                        ComputedColumnRewriter.CC_TO_REX_NODE_STR_CACHE.put(key, rexNodeStr);
                    }
                } catch (Exception ex) {
                    // do nothing
                }
            }
        });
    }
}

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

package org.apache.kylin.query.relnode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.kylin.common.NativeQueryRealization;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.fileseg.FileSegments;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.cuboid.NLookupCandidate;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.realization.HybridRealization;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.util.RexUtils;
import org.apache.kylin.util.CalciteSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class ContextUtil {

    static final ThreadLocal<OlapRel> _inputRel = new ThreadLocal<>();
    static final ThreadLocal<RelDataType> _resultType = new ThreadLocal<>();

    static final ThreadLocal<Map<String, String>> _localParameters = new ThreadLocal<>();
    static final ThreadLocal<Map<Integer, OlapContext>> _localContexts = new ThreadLocal<>();

    static final Map<NLookupCandidate.Policy, String> TYPE_MAPPING = ImmutableMap.of(NLookupCandidate.Policy.SNAPSHOT,
            QueryMetrics.TABLE_SNAPSHOT, NLookupCandidate.Policy.INTERNAL_TABLE, QueryMetrics.INTERNAL_TABLE);
    private static final Logger log = LoggerFactory.getLogger(ContextUtil.class);

    private ContextUtil() {
    }

    public static void setOlapRel(OlapRel olapRel) {
        _inputRel.set(olapRel);
    }

    public static void setRowType(RelDataType relDataType) {
        _resultType.set(relDataType);
    }

    public static void clean() {
        _inputRel.remove();
        _resultType.remove();
    }

    public static void amendAllColsIfNoAgg(RelNode rel) {
        if (rel == null || ((OlapRel) rel).getContext() == null || rel instanceof OlapTableScan) {
            return;
        }

        OlapContext context = ((OlapRel) rel).getContext();
        // add columns of context's TopNode to context when there are no agg rel
        if (rel instanceof OlapProjectRel && !((OlapProjectRel) rel).isMerelyPermutation()) {
            ((OlapRel) rel).getColumnRowType().getSourceColumns().stream().flatMap(Collection::stream)
                    .filter(context::isOriginAndBelongToCtxTables).forEach(context.getAllColumns()::add);
        } else if (rel instanceof OlapValuesRel) {
            ((OlapRel) rel).getColumnRowType().getAllColumns().stream().filter(context::isOriginAndBelongToCtxTables)
                    .forEach(context.getAllColumns()::add);
        } else if (rel instanceof OlapWindowRel) {
            ((OlapWindowRel) rel).getGroupingColumns().stream().filter(context::isOriginAndBelongToCtxTables)
                    .forEach(context.getAllColumns()::add);
        } else if (rel instanceof OlapJoinRel) {
            amendAllColsIfNoAgg(rel.getInput(0));
            amendAllColsIfNoAgg(rel.getInput(1));
        } else {
            amendAllColsIfNoAgg(rel.getInput(0));
        }
    }

    /**
     * used for collect a rel node's all subContext, which contain the context of itself
     */
    public static Set<OlapContext> collectSubContext(RelNode subRel) {
        Set<OlapContext> subContexts = Sets.newHashSet();
        if (subRel == null)
            return subContexts;

        subContexts.addAll(((OlapRel) subRel).getSubContexts());
        if (((OlapRel) subRel).getContext() != null)
            subContexts.add(((OlapRel) subRel).getContext());
        return subContexts;
    }

    //pre-order travel to set subContexts
    public static void setSubContexts(RelNode relNode) {
        Set<OlapContext> subContexts = Sets.newHashSet();
        if (relNode == null)
            return;

        for (RelNode inputNode : relNode.getInputs()) {
            setSubContexts(inputNode);
            subContexts.addAll(collectSubContext(inputNode));
        }
        ((OlapRel) relNode).setSubContexts(subContexts);
    }

    public static void setParameters(Map<String, String> parameters) {
        _localParameters.set(parameters);
    }

    public static void clearParameter() {
        _localParameters.remove();
    }

    public static void registerContext(OlapContext ctx) {
        if (_localContexts.get() == null) {
            Map<Integer, OlapContext> contextMap = new HashMap<>();
            _localContexts.set(contextMap);
        }
        _localContexts.get().put(ctx.getId(), ctx);
    }

    public static Collection<OlapContext> getThreadLocalContexts() {
        Map<Integer, OlapContext> map = _localContexts.get();
        return map == null ? Collections.emptyList() : map.values();
    }

    public static List<OlapContext> listContexts() {
        return Lists.newArrayList(ContextUtil.getThreadLocalContexts());
    }

    public static OlapContext getThreadLocalContextById(int id) {
        Map<Integer, OlapContext> map = _localContexts.get();
        return map.get(id);
    }

    public static void clearThreadLocalContexts() {
        _localContexts.remove();
    }

    public static void clearThreadLocalContextById(int id) {
        Map<Integer, OlapContext> map = _localContexts.get();
        map.remove(id);
        _localContexts.set(map);
    }

    public static List<NativeQueryRealization> getNativeRealizations() {
        List<NativeQueryRealization> realizations = Lists.newArrayList();

        // contexts can be null in case of 'explain plan for'
        Collection<OlapContext> threadLocalContexts = ContextUtil.getThreadLocalContexts();
        for (OlapContext ctx : threadLocalContexts) {
            Map<String, NLookupCandidate.Policy> lookupTables = new HashMap<>();
            final String realizationType = detectType(ctx, lookupTables);
            IRealization realization = ctx.getRealization();
            if (realization != null) {
                // try to use fusion model alias
                String modelId = realization.getModel().getUuid();
                String modelAlias = realization.getModel().getFusionModelAlias();
                if (!ctx.getStorageContext().getStreamCandidate().isEmpty()) {
                    // for streaming realization
                    realizations.add(streamRlz(ctx, realizationType, modelId, modelAlias, lookupTables));
                    if (realization instanceof HybridRealization) {
                        HybridRealization batchRealization = (HybridRealization) realization;
                        String batchModelId = batchRealization.getBatchRealization().getUuid();
                        realizations.add(batchRlz(ctx, realizationType, batchModelId, modelAlias, lookupTables));
                    }
                } else {
                    // for batch realization
                    realizations.add(batchRlz(ctx, realizationType, modelId, modelAlias, lookupTables));
                }
            }

            if (!lookupTables.isEmpty()) {
                lookupTables.forEach((lookup, type) -> {
                    String lookupRealizationType = TYPE_MAPPING.get(type);
                    NativeQueryRealization lookupRealization = new NativeQueryRealization(lookup,
                            lookupRealizationType);
                    lookupRealization.setLookupTables(Lists.newArrayList(lookup));
                    realizations.add(lookupRealization);
                });
            }
        }
        return realizations;
    }

    private static String detectType(OlapContext ctx, Map<String, NLookupCandidate.Policy> lookupMap) {
        String realizationType;
        if (ctx.getStorageContext().isDataSkipped()) {
            if (ctx.getStorageContext().isFilterCondAlwaysFalse()) {
                realizationType = QueryMetrics.FILTER_CONFLICT;
            } else {
                realizationType = null;
            }
        } else if (ctx.getStorageContext().getLookupCandidate() != null) {
            realizationType = QueryMetrics.TABLE_SNAPSHOT;
            lookupMap.put(ctx.getFirstTableIdentity(), ctx.getStorageContext().getLookupCandidate().getPolicy());
        } else if (ctx.getStorageContext().getBatchCandidate().isTableIndex()) {
            realizationType = QueryMetrics.TABLE_INDEX;
        } else {
            realizationType = QueryMetrics.AGG_INDEX;
        }
        return realizationType;
    }

    private static NativeQueryRealization streamRlz(OlapContext ctx, String realizationType, String modelId,
            String modelAlias, Map<String, NLookupCandidate.Policy> lookupTables) {
        NativeQueryRealization streamingRealization = new NativeQueryRealization(modelId, modelAlias,
                ctx.getStorageContext().getStreamCandidate().getLayoutId(), realizationType,
                ctx.getStorageContext().isPartialMatch());
        streamingRealization.setStreamingLayout(true);
        // TODO
        log.debug(lookupTables.toString());
        return streamingRealization;
    }

    private static NativeQueryRealization batchRlz(OlapContext ctx, String realizationType, String modelId,
            String modelAlias, Map<String, NLookupCandidate.Policy> lookupTables) {
        val realization = new NativeQueryRealization(modelId, modelAlias,
                ctx.getStorageContext().getBatchCandidate().getLayoutId(), realizationType,
                ctx.getStorageContext().isPartialMatch());

        // lastDataLoadTime & isLoadingData
        if (ctx.getRealization() instanceof NDataflow) {
            NDataflow df = (NDataflow) ctx.getRealization();
            realization.setStorageType(df.getStorageType());
            NLookupCandidate.Policy policy = NLookupCandidate.getDerivedPolicy(df.getConfig());
            for (String derivedLookup : ctx.getStorageContext().getBatchCandidate().getDerivedLookups()) {
                lookupTables.put(derivedLookup, policy);
            }
            if (df.getModel().isFilePartitioned()) {
                boolean isLoadingData = df.getSegments().stream()
                        .anyMatch(seg -> seg.getStatus() == SegmentStatusEnum.NEW);
                realization.setLoadingData(isLoadingData);
                realization.setBuildingIndex(FileSegments.guessIsBuildingIndex(df));
                realization.setLastDataRefreshTime(df.getLastDataRefreshTime());
            }
        }

        return realization;
    }

    public static RexInputRef createUniqueInputRefAmongTables(OlapTableScan table, int columnIdx,
            Collection<OlapTableScan> tables) {
        List<TableScan> sorted = new ArrayList<>(tables);
        sorted.sort(Comparator.comparingInt(AbstractRelNode::getId));
        int offset = 0;
        for (TableScan tableScan : sorted) {
            if (tableScan == table) {
                return new RexInputRef(
                        table.getTableName() + "." + table.getRowType().getFieldList().get(columnIdx).getName(),
                        offset + columnIdx, table.getRowType().getFieldList().get(columnIdx).getType());
            }
            offset += tableScan.getRowType().getFieldCount();
        }
        return null;
    }

    public static List<OlapContext> listContextsHavingScan() {
        // Context has no table scan is created by OlapJoinRel which looks like
        //     (sub-query) as A join (sub-query) as B
        // No realization needed for such context.
        List<OlapContext> result = Lists.newArrayList();
        for (OlapContext ctx : ContextUtil.getThreadLocalContexts()) {
            if (ctx.getFirstTableScan() != null)
                result.add(ctx);
        }
        return result;
    }

    public static boolean qualifiedForAggInfoPushDown(RelNode currentRel, OlapContext subContext) {
        // 1. the parent node of TopRel in subContext is not NULL and is instance Of OlapJoinRel.
        // 2. the TopNode of subContext is NOT instance of OlapAggregateRel.
        // 3. JoinRels in the path from currentNode to the ParentOfContextTopRel
        //    node are all the same type (left/inner/cross)
        // 4. all aggregate is derived from the same subContext
        return (subContext.getParentOfTopNode() instanceof OlapJoinRel
                || subContext.getParentOfTopNode() instanceof OlapNonEquiJoinRel)
                && !(subContext.getTopNode() instanceof OlapAggregateRel)
                && areSubJoinRelsSameType(currentRel, subContext, null, null)
                && derivedFromSameContext(new HashSet<>(), currentRel, subContext, false);
    }

    public static void dumpCalcitePlan(String msg, RelNode relNode, Logger logger) {
        if (Boolean.TRUE.equals(CalciteSystemProperty.DEBUG.value()) && logger.isDebugEnabled()) {
            logger.debug("{} :{}{}", msg, System.lineSeparator(), RelOptUtil.toString(relNode));
        }
        if (QueryContext.current().isDryRun() && msg.contains("FIRST ROUND")) {
            QueryContext.current().setLastUsedRelNode(RelOptUtil.toString(relNode));
        }
    }

    private static boolean derivedFromSameContext(Collection<Integer> indexOfInputCols, RelNode currentNode,
            OlapContext subContext, boolean hasCountConstant) {
        if (currentNode instanceof OlapAggregateRel) {
            hasCountConstant = hasCountConstant((OlapAggregateRel) currentNode);
            Set<Integer> inputColsIndex = collectAggInputIndex(((OlapAggregateRel) currentNode));
            return derivedFromSameContext(inputColsIndex, ((OlapAggregateRel) currentNode).getInput(), subContext,
                    hasCountConstant);

        } else if (currentNode instanceof OlapProjectRel) {
            Set<RexNode> rexLiterals = indexOfInputCols.stream()
                    .map(index -> ((OlapProjectRel) currentNode).getRewriteProjects().get(index))
                    .filter(RexLiteral.class::isInstance).collect(Collectors.toSet());
            Set<Integer> indexOfInputRel = indexOfInputCols.stream()
                    .map(index -> ((OlapProjectRel) currentNode).getRewriteProjects().get(index))
                    .flatMap(rex -> RexUtils.getAllInputRefs(rex).stream()).map(RexSlot::getIndex)
                    .collect(Collectors.toSet());
            if (!indexOfInputCols.isEmpty() && indexOfInputRel.isEmpty() && rexLiterals.isEmpty()) {
                throw new IllegalStateException(
                        "Error on collection index, index " + indexOfInputCols + " child index " + indexOfInputRel);
            }
            return derivedFromSameContext(indexOfInputRel, ((OlapProjectRel) currentNode).getInput(), subContext,
                    hasCountConstant);

        } else if (currentNode instanceof OlapJoinRel || currentNode instanceof OlapNonEquiJoinRel) {
            return isJoinFromSameContext(indexOfInputCols, (Join) currentNode, subContext, hasCountConstant);

        } else if (currentNode instanceof OlapFilterRel) {
            RexNode condition = ((OlapFilterRel) currentNode).getCondition();
            if (condition instanceof RexCall)
                indexOfInputCols.addAll(collectColsFromFilterRel((RexCall) condition));
            return derivedFromSameContext(indexOfInputCols, ((OlapFilterRel) currentNode).getInput(), subContext,
                    hasCountConstant);

        } else {
            //https://github.com/Kyligence/KAP/issues/9952
            // do not support agg-push-down if WindowRel, SortRel, LimitRel, ValueRel is met
            return false;
        }
    }

    private static boolean hasCountConstant(OlapAggregateRel aggRel) {
        return aggRel.getAggregateCalls().stream().anyMatch(func -> !func.isDistinct() && func.getArgList().isEmpty()
                && func.getAggregation() instanceof SqlCountAggFunction);
    }

    private static Set<Integer> collectAggInputIndex(OlapAggregateRel aggRel) {
        Set<Integer> inputColsIndex = Sets.newHashSet();
        for (AggregateCall aggregateCall : aggRel.getAggregateCalls()) {
            if (aggregateCall.getArgList() == null)
                continue;
            inputColsIndex.addAll(aggregateCall.getArgList());
        }
        inputColsIndex.addAll(aggRel.getRewriteGroupKeys());
        return inputColsIndex;
    }

    private static boolean isJoinFromSameContext(Collection<Integer> indexOfInputCols, Join joinRel,
            OlapContext subContext, boolean hasCountConstant) {
        // now support Cartesian Join if children are from different contexts
        if (joinRel.getJoinType() == JoinRelType.LEFT && hasCountConstant)
            return false;
        if (indexOfInputCols.isEmpty())
            return true;
        int maxIndex = Collections.max(indexOfInputCols);
        int leftLength = joinRel.getLeft().getRowType().getFieldList().size();
        if (maxIndex < leftLength) {
            return isLeftJoinFromSameContext(indexOfInputCols, joinRel, subContext, hasCountConstant);
        }
        int minIndex = Collections.min(indexOfInputCols);
        if (minIndex >= leftLength) {
            return isRightJoinFromSameContext(indexOfInputCols, joinRel, subContext, hasCountConstant, leftLength);
        }
        return false;
    }

    private static boolean isLeftJoinFromSameContext(Collection<Integer> indexOfInputCols, Join joinRel,
            OlapContext subContext, boolean hasCountConstant) {
        OlapRel potentialSubRel = (OlapRel) joinRel.getLeft();
        if (subContext == potentialSubRel.getContext()) {
            return true;
        }
        if (potentialSubRel.getContext() != null) {
            return false;
        }
        if (potentialSubRel instanceof OlapProjectRel) {
            if (joinRel instanceof OlapJoinRel) {
                ((OlapJoinRel) joinRel).getLeftKeys().forEach(leftKey -> {
                    RexNode leftCol = ((OlapProjectRel) potentialSubRel).getProjects().get(leftKey);
                    if (leftCol instanceof RexCall) {
                        indexOfInputCols.add(leftKey);
                    }
                });
            } else {
                // non-equiv-join rel: treat the left and right subtrees as a context,
                // and refuse to push agg down.
                OlapNonEquiJoinRel nonEquivJoinRel = (OlapNonEquiJoinRel) joinRel;
                if (!nonEquivJoinRel.isScd2Rel()) {
                    return false;
                }
            }
        }
        return derivedFromSameContext(indexOfInputCols, potentialSubRel, subContext, hasCountConstant);
    }

    private static boolean isRightJoinFromSameContext(Collection<Integer> indexOfInputCols, Join joinRel,
            OlapContext subContext, boolean hasCountConstant, int leftLength) {
        OlapRel potentialSubRel = (OlapRel) joinRel.getRight();
        if (subContext == potentialSubRel.getContext()) {
            return true;
        }
        if (potentialSubRel.getContext() != null) {
            return false;
        }
        Set<Integer> indexOfInputRel = Sets.newHashSet();
        for (Integer indexOfInputCol : indexOfInputCols) {
            indexOfInputRel.add(indexOfInputCol - leftLength);
        }
        return derivedFromSameContext(indexOfInputRel, potentialSubRel, subContext, hasCountConstant);
    }

    private static boolean areSubJoinRelsSameType(RelNode olapRel, OlapContext subContext, JoinRelType expectedJoinType,
            Class<?> joinCondClz) {
        OlapContext ctx = ((OlapRel) olapRel).getContext();
        if (ctx != null && ctx != subContext)
            return false;

        if (olapRel instanceof Join) {
            Join joinRel = (Join) olapRel;
            if (joinCondClz == null) {
                joinCondClz = joinRel.getCondition().getClass();
            }
            if (expectedJoinType == null) {
                expectedJoinType = joinRel.getJoinType();
            }
            if (joinRel.getJoinType() == expectedJoinType && joinRel.getCondition().getClass().equals(joinCondClz)) {
                if (joinRel.getJoinType() == JoinRelType.INNER) {
                    return olapRel == subContext.getParentOfTopNode()
                            || areSubJoinRelsSameType(joinRel.getLeft(), subContext, expectedJoinType, joinCondClz)
                            || areSubJoinRelsSameType(joinRel.getRight(), subContext, expectedJoinType, joinCondClz);
                } else if (joinRel.getJoinType() == JoinRelType.LEFT) {
                    // left join can only judge the left side
                    return areSubJoinRelsSameType(joinRel.getLeft(), subContext, expectedJoinType, joinCondClz);
                }
            }
            return false;
        }
        return olapRel.getInputs().isEmpty()
                || areSubJoinRelsSameType(olapRel.getInput(0), subContext, expectedJoinType, joinCondClz);
    }

    private static Set<Integer> collectColsFromFilterRel(RexCall filterCondition) {
        return RexUtils.getAllInputRefs(filterCondition).stream().map(RexSlot::getIndex).collect(Collectors.toSet());
    }

    public static void updateSubContexts(Collection<TblColRef> colRefs, Set<OlapContext> subContexts) {
        colRefs.forEach(colRef -> {
            for (OlapContext context : subContexts) {
                if (colRef != null && context.belongToContextTables(colRef)) {
                    context.getAllColumns().add(colRef);
                }
            }
        });
    }
}

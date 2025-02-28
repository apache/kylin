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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.tools.RelUtils;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.schema.OlapTable;
import org.apache.kylin.query.util.ICutContextStrategy;
import org.apache.kylin.query.util.RexToTblColRefTranslator;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@Getter
public class OlapProjectRel extends Project implements OlapRel {

    private List<RexNode> rewriteProjects;
    private OlapContext context;
    private Set<OlapContext> subContexts = Sets.newHashSet();
    @Getter(AccessLevel.PRIVATE)
    private boolean rewriting;
    private ColumnRowType columnRowType;
    @Getter(AccessLevel.PRIVATE)
    private boolean afterAggregate;

    /** project additionally added by OlapJoinPushThroughJoinRule */
    private boolean isMerelyPermutation = false;
    @Getter(AccessLevel.PRIVATE)
    private int caseCount = 0;
    @Getter(AccessLevel.PRIVATE)
    private boolean beforeTopPreCalcJoin = false;
    @Setter
    private boolean needPushInfoToSubCtx = false;

    public OlapProjectRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, List<RexNode> exps,
            RelDataType rowType) {
        super(cluster, traitSet, child, exps, rowType);
        Preconditions.checkArgument(getConvention() == CONVENTION);
        this.rewriteProjects = exps;
        this.rowType = getRowType();
        for (RexNode exp : exps) {
            caseCount += RelUtils.countOperatorCall(SqlCaseOperator.INSTANCE, exp);
        }
    }

    @Override
    public List<RexNode> getProjects() {
        return rewriteProjects;
    }

    private ColumnRowType buildColumnRowType() {
        List<TblColRef> columns = Lists.newArrayList();
        List<Set<TblColRef>> sourceColumns = Lists.newArrayList();
        OlapRel olapChild = (OlapRel) getInput();
        ColumnRowType inputColumnRowType = olapChild.getColumnRowType();
        Map<RexNode, TblColRef> nodeAndTblColMap = new HashMap<>();
        for (int i = 0; i < this.rewriteProjects.size(); i++) {
            RexNode rex = this.rewriteProjects.get(i);
            RelDataTypeField columnField = this.rowType.getFieldList().get(i);
            String fieldName = columnField.getName();
            Set<TblColRef> sourceCollector = Sets.newHashSet();
            TblColRef column = translateRexNode(rex, inputColumnRowType, fieldName, sourceCollector, nodeAndTblColMap);
            if (column == null) {
                throw new IllegalStateException("No TblColRef found in " + rex);
            }
            columns.add(column);
            sourceColumns.add(sourceCollector);
        }
        return new ColumnRowType(columns, sourceColumns);
    }

    TblColRef translateRexNode(RexNode rexNode, ColumnRowType inputColumnRowType, String fieldName,
            Set<TblColRef> sourceCollector, Map<RexNode, TblColRef> nodeAndTblColMap) {
        if (!this.afterAggregate) {
            return RexToTblColRefTranslator.translateRexNode(rexNode, inputColumnRowType, fieldName, sourceCollector,
                    nodeAndTblColMap);
        } else {
            return RexToTblColRefTranslator.translateRexNode(rexNode, inputColumnRowType, fieldName, nodeAndTblColMap);
        }
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        if (getInput() instanceof OlapFilterRel) {
            // merge project & filter
            OlapFilterRel filter = (OlapFilterRel) getInput();
            RelNode inputOfFilter = inputs.get(0).getInput(0);
            RexProgram program = RexProgram.create(inputOfFilter.getRowType(), this.rewriteProjects,
                    filter.getCondition(), this.rowType, getCluster().getRexBuilder());
            return new EnumerableCalc(getCluster(), getCluster().traitSetOf(EnumerableConvention.INSTANCE), //
                    inputOfFilter, program);
        } else {
            // keep project for table scan
            EnumerableRel input = sole(inputs);
            RexProgram program = RexProgram.create(input.getRowType(), this.rewriteProjects, null, this.rowType,
                    getCluster().getRexBuilder());
            return new EnumerableCalc(getCluster(), getCluster().traitSetOf(EnumerableConvention.INSTANCE), //
                    input, program);
        }
    }

    @Override
    public boolean hasSubQuery() {
        OlapRel olapChild = (OlapRel) getInput();
        return olapChild.hasSubQuery();
    }

    @Override
    public RelTraitSet replaceTraitSet(RelTrait trait) {
        RelTraitSet oldTraitSet = this.traitSet;
        this.traitSet = this.traitSet.replace(trait);
        return oldTraitSet;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        pw.input("input", getInput());
        if (pw.nest()) {
            pw.item("fields", rowType.getFieldNames());
            pw.item("exprs", rewriteProjects);
        } else {
            for (Ord<RelDataTypeField> field : Ord.zip(rowType.getFieldList())) {
                String fieldName = field.e.getName();
                if (fieldName == null) {
                    fieldName = "field#" + field.i;
                }
                pw.item(fieldName, rewriteProjects.get(field.i));
            }
        }

        if (context != null) {
            pw.item("ctx", displayCtxId(context));
            if (context.getGroupByColumns() != null && context.getReturnTupleInfo() != null
                    && context.getReturnTupleInfo().getColumnMap() != null) {
                List<Integer> colIds = context.getGroupByColumns().stream()
                        .map(colRef -> context.getReturnTupleInfo().getColumnMap().get(colRef)) //
                        .collect(Collectors.toList());
                pw.item("groupByColumns", colIds);
            }
        } else {
            pw.item("ctx", "");
        }
        return pw;
    }

    @Override
    public void implementCutContext(ICutContextStrategy.ContextCutImpl contextCutImpl) {
        this.context = null;
        this.columnRowType = null;
        contextCutImpl.visitChild(getInput());
    }

    /**
     * Since the project under aggregate maybe reduce expressions by AggregateProjectReduceRule,
     * consider the count of expressions into cost, the reduced project will be used.
     * <p>
     * Made RexOver much more expensive, so we can transform into {@link OlapWindowRel}
     * by rules in {@link org.apache.calcite.rel.rules.ProjectToWindowRule}
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        boolean hasRexOver = RexOver.containsOver(getProjects(), null);
        RelOptCost relOptCost = super.computeSelfCost(planner, mq) //
                .multiplyBy(OlapRel.OLAP_COST_FACTOR) //
                .multiplyBy(getComplexity()) //
                .multiplyBy(getProjects().size() * (hasRexOver ? 50.0 : 1.0))
                .plus(planner.getCostFactory().makeCost(0.1 * caseCount, 0, 0));
        return planner.getCostFactory().makeCost(relOptCost.getRows(), 0, 0);
    }

    // Simplified OlapProjectRel has lower cost. For example:
    // RexNode(1+1) will be converted to RexNode(2) by ReduceExpressionRule,
    // thus we should choose the latter because it has lower complexity.
    private double getComplexity() {
        int complexity = 1;
        for (RexNode rexNode : getProjects()) {
            complexity += rexNode.toString().length();
        }
        return Math.exp(-1.0 / complexity);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode child, List<RexNode> exps, RelDataType rowType) {
        return new OlapProjectRel(getCluster(), traitSet, child, exps, rowType);
    }

    @Override
    public void setContext(OlapContext context) {
        this.context = context;
        ((OlapRel) getInput()).setContext(context);
        subContexts.addAll(ContextUtil.collectSubContext(this.getInput()));
    }

    @Override
    public boolean pushRelInfoToContext(OlapContext context) {
        if (this.context == null && ((OlapRel) getInput()).pushRelInfoToContext(context)) {
            this.context = context;
            return true;
        }
        return false;
    }

    @Override
    public void implementContext(ContextImpl contextImpl, ContextVisitorState state) {
        contextImpl.fixSharedOlapTableScan(this);
        ContextVisitorState tempState = ContextVisitorState.init();
        contextImpl.visitChild(getInput(), this, tempState);
        subContexts.addAll(ContextUtil.collectSubContext(this.getInput()));
        if (context == null && subContexts.size() == 1
                && this.getInput() == Lists.newArrayList(this.subContexts).get(0).getTopNode()
                && !(this.getInput() instanceof OlapWindowRel)) {
            this.context = Lists.newArrayList(this.subContexts).get(0);
            this.context.setTopNode(this);
        }
        state.merge(tempState);
    }

    @Override
    public void implementOlap(OlapImpl olapImpl) {
        if (this.getPermutation() != null && !isTopProject(olapImpl.getParentNodeStack()))
            isMerelyPermutation = true;
        // @beforeTopPreCalcJoin refer to this rel is under a preCalcJoin Rel and need not be rewritten. e.g.
        //        JOIN
        //       /    \
        //     Proj   TableScan
        //    /
        //  TableScan
        this.beforeTopPreCalcJoin = context != null && context.isHasPreCalcJoin();
        olapImpl.visitChild(getInput(), this);

        this.columnRowType = buildColumnRowType();
        if (context != null) {
            this.afterAggregate = context.isAfterAggregate();
            if (this == context.getTopNode() && !context.isHasAgg()) {
                ContextUtil.amendAllColsIfNoAgg(this);
            }
        } else if (this.needPushInfoToSubCtx) {
            updateSubContexts(subContexts);
        }
    }

    private boolean isTopProject(Deque<RelNode> parentNodeStack) {
        Deque<RelNode> tmpStack = new ArrayDeque<>(parentNodeStack);
        while (!tmpStack.isEmpty()) {
            RelNode parentNode = tmpStack.pollFirst();
            if (parentNode instanceof OlapToEnumerableConverter) {
                return true;
            }
            if (parentNode instanceof OlapProjectRel) {
                return false;
            }
        }
        return false;
    }

    @Override
    public void implementRewrite(RewriteImpl rewriteImpl) {
        rewriteImpl.visitChild(this, getInput());
        if (this.context == null) {
            return;
        }
        this.rewriting = true;

        // project before join or is just after OlapToEnumerableConverter
        if (!RewriteImpl.needRewrite(this.context) || this.afterAggregate || !(this.context.hasPrecalculatedFields())
                || (this.getContext().isHasJoin() && this.beforeTopPreCalcJoin)) {
            this.columnRowType = this.buildColumnRowType();
            this.rewriteProjects();
            return;
        }
        List<RelDataTypeField> newFieldList = Lists.newArrayList();
        Map<Integer, RelDataTypeField> toBeReplacedCcMap = replaceGroupByExpsWithCcField(newFieldList);
        newFieldList.addAll(rebuildMissPreCalcField());

        // rebuild rowType
        List<RelDataTypeField> originFields = Lists.newArrayList(this.rowType.getFieldList());
        toBeReplacedCcMap.forEach(originFields::set);
        if (!newFieldList.isEmpty()) {
            List<RelDataTypeField> fieldList = Stream.of(originFields, newFieldList) //
                    .flatMap(List::stream).collect(Collectors.toList());
            this.rowType = getCluster().getTypeFactory().createStructType(fieldList);
        }
        // rebuild columns
        this.columnRowType = this.buildColumnRowType();
        this.rewriteProjects();
        this.rewriting = false;
    }

    private void rewriteProjects() {
        OlapRel olapChild = (OlapRel) getInput();
        ColumnRowType inputColumnRowType = olapChild.getColumnRowType();
        List<TblColRef> allColumns = inputColumnRowType.getAllColumns();
        List<TblColRef> ccColRefList = allColumns.stream() //
                .filter(col -> col.getColumnDesc().isComputedColumn()) //
                .collect(Collectors.toList());

        Map<TblColRef, Integer> columnToIdMap = Maps.newHashMap();
        for (int i = 0; i < allColumns.size(); i++) {
            TblColRef colRef = allColumns.get(i);
            if (TblColRef.UNKNOWN_ALIAS.equalsIgnoreCase(colRef.getTableAlias())) {
                continue;
            } else if (columnToIdMap.containsKey(colRef)) {
                logger.warn("duplicate TblColRef {} of computed column.", colRef);
            }
            columnToIdMap.putIfAbsent(colRef, i);
        }
        List<RexNode> newRewriteProjList = Lists.newArrayList();
        Map<String, TblColRef> map = Maps.newHashMap();
        for (TblColRef tblColRef : ccColRefList) {
            map.putIfAbsent(tblColRef.getDoubleQuoteExp(), tblColRef);
        }
        Map<RexNode, TblColRef> nodeAndTblColMap = new HashMap<>();
        for (int i = 0; i < this.rewriteProjects.size(); i++) {
            RexNode rex = this.rewriteProjects.get(i);
            RelDataTypeField columnField = this.rowType.getFieldList().get(i);
            String fieldName = columnField.getName();
            Set<TblColRef> sourceCollector = Sets.newHashSet();
            TblColRef column = translateRexNode(rex, inputColumnRowType, fieldName, sourceCollector, nodeAndTblColMap);
            if (column == null)
                throw new IllegalStateException("No TblColRef found in " + rex);
            TblColRef existColRef = map.get(column.toString());
            if (existColRef != null && getContext().getAllColumns().contains(existColRef)) {
                column = existColRef;
                List<RelDataTypeField> inputFieldList = getInput().getRowType().getFieldList();
                RelDataTypeField inputField = inputFieldList.get(columnToIdMap.get(column));
                RexNode newRef = inputField == null ? rex
                        : new RexInputRef(inputField.getIndex(), inputField.getType());
                newRewriteProjList.add(newRef);
            } else {
                newRewriteProjList.add(rex);
            }
        }
        this.rewriteProjects = newRewriteProjList;
    }

    private void updateSubContexts(Set<OlapContext> subContexts) {
        if (isMerelyPermutation || this.rewriting || this.afterAggregate)
            return;

        ContextUtil.updateSubContexts(
                this.columnRowType.getSourceColumns().stream().flatMap(Collection::stream).collect(Collectors.toSet()),
                subContexts);
    }

    @Override
    public void setSubContexts(Set<OlapContext> contexts) {
        this.subContexts = contexts;
    }

    private Map<Integer, RelDataTypeField> replaceGroupByExpsWithCcField(List<RelDataTypeField> newFieldList) {
        Map<Integer, RelDataTypeField> toBeReplacedCCMap = Maps.newHashMap();
        Map<Integer, RexNode> posInTupleToCcCol = Maps.newHashMap();
        ColumnRowType inputColumnRowType = ((OlapRel) getInput()).getColumnRowType();
        AtomicInteger paramIndex = new AtomicInteger(this.rowType.getFieldList().size());
        context.getGroupCCColRewriteMapping().forEach((originExp, ccRef) -> {
            String replaceCCField = ccRef.getName();
            int rowIndex = this.columnRowType.getColumnIndex(this.context, replaceCCField);
            if (rowIndex >= 0) {
                return;
            }

            RelDataType ccFieldType = OlapTable.createSqlType(getCluster().getTypeFactory(), ccRef.getType(), true);
            int ccColInInputIndex = inputColumnRowType.getColumnIndex(this.context, replaceCCField);
            RelDataTypeField inputField = getInput().getRowType().getFieldList().get(ccColInInputIndex);
            int originExprIndex = findColumnRowTypePosition(originExp, this);
            if (originExprIndex < 0) {
                newFieldList.add(new RelDataTypeFieldImpl(replaceCCField, paramIndex.getAndIncrement(), ccFieldType));
                List<RexNode> newRewriteProjects = Lists.newArrayList(this.rewriteProjects);
                newRewriteProjects.add(new RexInputRef(inputField.getIndex(), inputField.getType()));
                this.rewriteProjects = newRewriteProjects;
            } else {
                RelDataTypeField newCcFiled = new RelDataTypeFieldImpl(replaceCCField, originExprIndex, ccFieldType);
                toBeReplacedCCMap.put(originExprIndex, newCcFiled);
                RexInputRef ccFiledRef = new RexInputRef(inputField.getIndex(), inputField.getType());
                posInTupleToCcCol.put(originExprIndex, ccFiledRef);
            }
        });
        if (!posInTupleToCcCol.isEmpty()) {
            List<RexNode> newProjects = new ArrayList<>(this.rewriteProjects);
            posInTupleToCcCol.forEach(newProjects::set);
            this.rewriteProjects = newProjects;
        }
        return toBeReplacedCCMap;
    }

    private List<RelDataTypeField> rebuildMissPreCalcField() {
        List<RelDataTypeField> newFieldList = Lists.newLinkedList();
        List<RexNode> newExpList = Lists.newArrayList();
        List<RelDataTypeField> inputFieldList = getInput().getRowType().getFieldList();
        ColumnRowType inputColumnRowType = ((OlapRel) getInput()).getColumnRowType();

        // rebuild all rewriteProjects with source input
        List<TblColRef> allColumns = this.columnRowType.getAllColumns();
        for (int i = 0; i < this.rewriteProjects.size(); i++) {
            RexNode rexNode = this.rewriteProjects.get(i);
            if (i >= allColumns.size() || !(rexNode instanceof RexInputRef)) {
                newExpList.add(rexNode);
                continue;
            }

            String inputColumnName = inputColumnRowType.getAllColumns() //
                    .get(((RexInputRef) rexNode).getIndex()).getCanonicalName();
            String currentColumnName = allColumns.get(i).getCanonicalName();
            int actualIndex = inputColumnRowType.getIndexByCanonicalName(currentColumnName);
            if (!inputColumnName.equals(currentColumnName) && actualIndex >= 0) {
                // need rebuild
                RelDataTypeField inputField = inputFieldList.get(actualIndex);
                RexInputRef newFieldRef = new RexInputRef(actualIndex, inputField.getType());
                newExpList.add(newFieldRef);
            } else {
                newExpList.add(rexNode);
            }
        }

        // rebuild pre-calculate column
        int paramIndex = this.rowType.getFieldList().size();
        for (Map.Entry<String, RelDataType> rewriteField : this.context.getRewriteFields().entrySet()) {
            String rewriteFieldName = rewriteField.getKey();
            int rowIndex = this.columnRowType.getColumnIndex(this.context, rewriteFieldName);
            if (rowIndex >= 0) {
                continue;
            }
            int inputIndex = inputColumnRowType.getColumnIndex(this.context, rewriteFieldName);
            if (inputIndex >= 0) {
                // new field
                RelDataType fieldType = rewriteField.getValue();
                RelDataTypeField newField = new RelDataTypeFieldImpl(rewriteFieldName, paramIndex++, fieldType);
                newFieldList.add(newField);
                // new project
                RelDataTypeField inputField = inputFieldList.get(inputIndex);
                RexInputRef newFieldRef = new RexInputRef(inputField.getIndex(), inputField.getType());
                newExpList.add(newFieldRef);
            }
        }

        this.rewriteProjects = newExpList;
        return newFieldList;
    }

    private int findColumnRowTypePosition(TblColRef colRef, OlapProjectRel rel) {
        for (int i = 0; i < rel.getColumnRowType().getAllColumns().size(); i++) {
            if (colRef.equals(rel.getColumnRowType().getColumnByIndex(i))) {
                return i;
            }
        }
        return -1;
    }
}

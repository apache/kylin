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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
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
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.util.RexToTblColRefTranslator;

/**
 */
public class OLAPProjectRel extends Project implements OLAPRel {

    public List<RexNode> rewriteProjects;
    protected OLAPContext context;
    protected boolean rewriting;
    protected ColumnRowType columnRowType;
    protected boolean hasJoin;
    protected boolean afterTopJoin;
    protected boolean afterAggregate;
    protected boolean isMerelyPermutation = false;//project additionally added by OLAPJoinPushThroughJoinRule
    private int caseCount = 0;

    public OLAPProjectRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, List<RexNode> exps,
            RelDataType rowType) {
        super(cluster, traitSet, child, exps, rowType);
        Preconditions.checkArgument(getConvention() == CONVENTION);
        this.rewriteProjects = exps;
        this.hasJoin = false;
        this.afterTopJoin = false;
        this.rowType = getRowType();
        for (RexNode exp : exps) {
            caseCount += RelUtils.countOperatorCall(SqlCaseOperator.INSTANCE, exp);
        }
    }

    @Override
    public List<RexNode> getChildExps() {
        return rewriteProjects;
    }

    @Override
    public List<RexNode> getProjects() {
        return rewriteProjects;
    }

    /**
     * Since the project under aggregate maybe reduce expressions by {@link org.apache.kylin.query.optrule.AggregateProjectReduceRule},
     * consider the count of expressions into cost, the reduced project will be used.
     *
     * Made RexOver much more expensive so we can transform into {@link OLAPWindowRel}
     * by rules in {@link org.apache.calcite.rel.rules.ProjectToWindowRule}
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        boolean hasRexOver = RexOver.containsOver(getProjects(), null);
        RelOptCost relOptCost = super.computeSelfCost(planner, mq).multiplyBy(.05)
                .multiplyBy(getProjects().size() * (hasRexOver ? 50.0 : 1.0))
                .plus(planner.getCostFactory().makeCost(0.1 * caseCount, 0, 0));
        return planner.getCostFactory().makeCost(relOptCost.getRows(), 0, 0);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode child, List<RexNode> exps, RelDataType rowType) {
        return new OLAPProjectRel(getCluster(), traitSet, child, exps, rowType);
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        if (this.getPermutation() != null && !(implementor.getParentNode() instanceof OLAPToEnumerableConverter)) {
            isMerelyPermutation = true;
        }

        implementor.fixSharedOlapTableScan(this);
        implementor.visitChild(getInput(), this);

        this.context = implementor.getContext();
        this.hasJoin = context.isHasJoin();
        this.afterTopJoin = context.afterTopJoin;
        this.afterAggregate = context.afterAggregate;

        this.columnRowType = buildColumnRowType();
        for (Set<TblColRef> colRefSet : columnRowType.getSourceColumns()) {
            for (TblColRef colRef : colRefSet) {
                if (!isMerelyPermutation && context.belongToContextTables(colRef)) {
                    context.allColumns.add(colRef);
                }
            }
        }
    }

    protected ColumnRowType buildColumnRowType() {
        List<TblColRef> columns = Lists.newArrayList();
        List<Set<TblColRef>> sourceColumns = Lists.newArrayList();
        OLAPRel olapChild = (OLAPRel) getInput();
        ColumnRowType inputColumnRowType = olapChild.getColumnRowType();
        Map<RexNode, TblColRef> nodeAndTblColMap = new HashMap<>();
        for (int i = 0; i < this.rewriteProjects.size(); i++) {
            RexNode rex = this.rewriteProjects.get(i);
            RelDataTypeField columnField = this.rowType.getFieldList().get(i);
            String fieldName = columnField.getName();
            Set<TblColRef> sourceCollector = Sets.newHashSet();
            TblColRef column = translateRexNode(rex, inputColumnRowType, fieldName, sourceCollector, nodeAndTblColMap);
            if (column == null)
                throw new IllegalStateException("No TblColRef found in " + rex);
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
        if (getInput() instanceof OLAPFilterRel) {
            // merge project & filter
            OLAPFilterRel filter = (OLAPFilterRel) getInput();
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
    public ColumnRowType getColumnRowType() {
        return columnRowType;
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, getInput());

        this.rewriting = true;

        // project before join or is just after OLAPToEnumerableConverter
        if (!RewriteImplementor.needRewrite(this.context) || (this.hasJoin && !this.afterTopJoin) || this.afterAggregate
                || !(this.context.hasPrecalculatedFields())) {
            this.columnRowType = this.buildColumnRowType();
            return;
        }

        // find missed rewrite fields
        int paramIndex = this.rowType.getFieldList().size();
        List<RelDataTypeField> newFieldList = Lists.newLinkedList();
        List<RexNode> newExpList = Lists.newLinkedList();
        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();

        for (Map.Entry<String, RelDataType> rewriteField : this.context.rewriteFields.entrySet()) {
            String rewriteFieldName = rewriteField.getKey();
            int rowIndex = this.columnRowType.getIndexByName(rewriteFieldName);
            if (rowIndex < 0) {
                int inputIndex = inputColumnRowType.getIndexByName(rewriteFieldName);
                if (inputIndex >= 0) {
                    // new field
                    RelDataType fieldType = rewriteField.getValue();
                    RelDataTypeField newField = new RelDataTypeFieldImpl(rewriteFieldName, paramIndex++, fieldType);
                    newFieldList.add(newField);
                    // new project
                    RelDataTypeField inputField = getInput().getRowType().getFieldList().get(inputIndex);
                    RexInputRef newFieldRef = new RexInputRef(inputField.getIndex(), inputField.getType());
                    newExpList.add(newFieldRef);
                }
            }
        }

        if (!newFieldList.isEmpty()) {
            // rebuild projects
            List<RexNode> newProjects = Lists.newArrayList(this.rewriteProjects);
            newProjects.addAll(newExpList);
            this.rewriteProjects = newProjects;

            // rebuild row type
            FieldInfoBuilder fieldInfo = getCluster().getTypeFactory().builder();
            fieldInfo.addAll(this.rowType.getFieldList());
            fieldInfo.addAll(newFieldList);
            this.rowType = getCluster().getTypeFactory().createStructType(fieldInfo);
        }

        // rebuild columns
        this.columnRowType = this.buildColumnRowType();

        this.rewriting = false;
    }

    @Override
    public OLAPContext getContext() {
        return context;
    }

    @Override
    public boolean hasSubQuery() {
        OLAPRel olapChild = (OLAPRel) getInput();
        return olapChild.hasSubQuery();
    }

    @Override
    public RelTraitSet replaceTraitSet(RelTrait trait) {
        RelTraitSet oldTraitSet = this.traitSet;
        this.traitSet = this.traitSet.replace(trait);
        return oldTraitSet;
    }

    public boolean isMerelyPermutation() {
        return isMerelyPermutation;
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
            pw.item("ctx", String.valueOf(context.id) + "@" + context.realization);
            if (context.getGroupByColumns() != null && context.returnTupleInfo != null
                    && context.returnTupleInfo.getColumnMap() != null) {
                List<Integer> colIds = context.getGroupByColumns().stream()
                        .map(colRef -> context.returnTupleInfo.getColumnMap().get(colRef)).collect(Collectors.toList());
                pw.item("groupByColumns", colIds);
            }
        } else {
            pw.item("ctx", "");
        }
        return pw;
    }
}

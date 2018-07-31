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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.expression.ColumnTupleExpression;
import org.apache.kylin.metadata.expression.ExpressionColCollector;
import org.apache.kylin.metadata.expression.NoneTupleExpression;
import org.apache.kylin.metadata.expression.NumberTupleExpression;
import org.apache.kylin.metadata.expression.StringTupleExpression;
import org.apache.kylin.metadata.expression.TupleExpression;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.TblColRef.InnerDataTypeEnum;
import org.apache.kylin.query.relnode.visitor.TupleExpressionVisitor;
import org.apache.kylin.query.schema.OLAPTable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
public class OLAPProjectRel extends Project implements OLAPRel {

    OLAPContext context;
    public List<RexNode> rewriteProjects;
    boolean rewriting;
    ColumnRowType columnRowType;
    boolean hasJoin;
    boolean afterJoin;
    boolean afterAggregate;
    boolean isMerelyPermutation = false;//project additionally added by OLAPJoinPushThroughJoinRule
    private int caseCount = 0;

    public OLAPProjectRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, List<RexNode> exps,
            RelDataType rowType) {
        super(cluster, traitSet, child, exps, rowType);
        Preconditions.checkArgument(getConvention() == OLAPRel.CONVENTION);
        Preconditions.checkArgument(child.getConvention() == OLAPRel.CONVENTION);
        this.rewriteProjects = exps;
        this.hasJoin = false;
        this.afterJoin = false;
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
     * Made RexOver much more expensive so we can transform into {@link org.apache.kylin.query.relnode.OLAPWindowRel}
     * by rules in {@link org.apache.calcite.rel.rules.ProjectToWindowRule}
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        boolean hasRexOver = RexOver.containsOver(getProjects(), null);
        RelOptCost relOptCost = super.computeSelfCost(planner, mq).multiplyBy(.05)
                .multiplyBy(getProjects().size() * (hasRexOver ? 50 : 1))
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
        this.hasJoin = context.hasJoin;
        this.afterJoin = context.afterJoin;
        this.afterAggregate = context.afterAggregate;

        this.columnRowType = buildColumnRowType();
    }

    ColumnRowType buildColumnRowType() {
        List<TblColRef> columns = Lists.newArrayList();
        List<TupleExpression> sourceColumns = Lists.newArrayList();

        OLAPRel olapChild = (OLAPRel) getInput();
        ColumnRowType inputColumnRowType = olapChild.getColumnRowType();
        boolean ifVerify = !hasSubQuery() && !afterAggregate;
        TupleExpressionVisitor visitor = new TupleExpressionVisitor(inputColumnRowType, ifVerify);
        for (int i = 0; i < this.rewriteProjects.size(); i++) {
            RexNode rex = this.rewriteProjects.get(i);
            RelDataTypeField columnField = this.rowType.getFieldList().get(i);
            String fieldName = columnField.getName();

            TupleExpression tupleExpr = rex.accept(visitor);
            TblColRef column = translateRexNode(tupleExpr, fieldName);
            if (!this.rewriting && !this.afterAggregate && !isMerelyPermutation) {
                Set<TblColRef> srcCols = ExpressionColCollector.collectColumns(tupleExpr);
                // remove cols not belonging to context tables
                Iterator<TblColRef> srcColIter = srcCols.iterator();
                while (srcColIter.hasNext()) {
                    if (!context.belongToContextTables(srcColIter.next())) {
                        srcColIter.remove();
                    }
                }
                this.context.allColumns.addAll(srcCols);

                if (this.context.isDynamicColumnEnabled() && tupleExpr.ifForDynamicColumn()) {
                    SqlTypeName fSqlType = columnField.getType().getSqlTypeName();
                    String dataType = OLAPTable.DATATYPE_MAPPING.get(fSqlType);
                    // upgrade data type for number columns
                    if (DataType.isNumberFamily(dataType)) {
                        dataType = "decimal";
                    }
                    column.getColumnDesc().setDatatype(dataType);
                    this.context.dynamicFields.put(column, columnField.getType());
                }
            } else {
                tupleExpr = new NoneTupleExpression();
            }

            columns.add(column);
            sourceColumns.add(tupleExpr);
        }
        return new ColumnRowType(columns, sourceColumns);
    }

    private TblColRef translateRexNode(TupleExpression tupleExpr, String fieldName) {
        if (tupleExpr instanceof ColumnTupleExpression) {
            return ((ColumnTupleExpression) tupleExpr).getColumn();
        } else if (tupleExpr instanceof NumberTupleExpression) {
            Object value = ((NumberTupleExpression) tupleExpr).getValue();
            return TblColRef.newInnerColumn(value == null ? "null" : value.toString(), InnerDataTypeEnum.LITERAL);
        } else if (tupleExpr instanceof StringTupleExpression) {
            Object value = ((StringTupleExpression) tupleExpr).getValue();
            return TblColRef.newInnerColumn(value == null ? "null" : value.toString(), InnerDataTypeEnum.LITERAL);
        }
        return TblColRef.newInnerColumn(fieldName, InnerDataTypeEnum.LITERAL, tupleExpr.getDigest());
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
        if (!RewriteImplementor.needRewrite(this.context) || (this.hasJoin && !this.afterJoin) || this.afterAggregate
                || !(this.context.hasPrecalculatedFields())) {
            this.columnRowType = this.buildColumnRowType();
            return;
        }

        List<RelDataTypeField> newFieldList = Lists.newLinkedList();
        List<RexNode> newExpList = Lists.newLinkedList();
        Map<Integer, Pair<RelDataTypeField, RexNode>> replaceFieldMap = Maps
                .newHashMapWithExpectedSize(this.context.dynamicFields.size());

        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();

        // find missed rewrite fields
        int paramIndex = this.rowType.getFieldList().size();
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

        // replace projects with dynamic fields
        Map<TblColRef, RelDataType> dynFields = this.context.dynamicFields;
        for (TblColRef dynFieldCol : dynFields.keySet()) {
            String replaceFieldName = dynFieldCol.getName();
            int rowIndex = this.columnRowType.getIndexByName(replaceFieldName);
            if (rowIndex >= 0) {
                int inputIndex = inputColumnRowType.getIndexByName(replaceFieldName);
                if (inputIndex >= 0) {
                    // field to be replaced
                    RelDataType fieldType = dynFields.get(dynFieldCol);
                    RelDataTypeField newField = new RelDataTypeFieldImpl(replaceFieldName, rowIndex, fieldType);
                    // project to be replaced
                    RelDataTypeField inputField = getInput().getRowType().getFieldList().get(inputIndex);
                    RexInputRef newFieldRef = new RexInputRef(inputField.getIndex(), inputField.getType());

                    replaceFieldMap.put(rowIndex, new Pair<RelDataTypeField, RexNode>(newField, newFieldRef));
                }
            }
        }

        if (!newFieldList.isEmpty() || !replaceFieldMap.isEmpty()) {
            List<RexNode> newProjects = Lists.newArrayList(this.rewriteProjects);
            List<RelDataTypeField> newFields = Lists.newArrayList(this.rowType.getFieldList());
            for (int rowIndex : replaceFieldMap.keySet()) {
                Pair<RelDataTypeField, RexNode> entry = replaceFieldMap.get(rowIndex);
                newProjects.set(rowIndex, entry.getSecond());
                newFields.set(rowIndex, entry.getFirst());
            }

            // rebuild projects
            newProjects.addAll(newExpList);
            this.rewriteProjects = newProjects;

            // rebuild row type
            FieldInfoBuilder fieldInfo = getCluster().getTypeFactory().builder();
            fieldInfo.addAll(newFields);
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
        return super.explainTerms(pw).item("ctx",
                context == null ? "" : String.valueOf(context.id) + "@" + context.realization);
    }
}

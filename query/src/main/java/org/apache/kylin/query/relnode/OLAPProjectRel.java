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
import java.util.HashSet;
import java.util.LinkedList;
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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.TblColRef.InnerDataTypeEnum;

import com.google.common.base.Preconditions;

/**
 */
public class OLAPProjectRel extends Project implements OLAPRel {

    private OLAPContext context;
    private List<RexNode> rewriteProjects;
    private boolean rewriting;
    private ColumnRowType columnRowType;
    private boolean hasJoin;
    private boolean afterJoin;
    private boolean afterAggregate;

    public OLAPProjectRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, List<RexNode> exps, RelDataType rowType) {
        super(cluster, traitSet, child, exps, rowType);
        Preconditions.checkArgument(getConvention() == OLAPRel.CONVENTION);
        Preconditions.checkArgument(child.getConvention() == OLAPRel.CONVENTION);
        this.rewriteProjects = exps;
        this.hasJoin = false;
        this.afterJoin = false;
        this.rowType = getRowType();
    }

    @Override
    public List<RexNode> getChildExps() {
        return rewriteProjects;
    }

    @Override
    public List<RexNode> getProjects() {
        return rewriteProjects;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.05);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode child, List<RexNode> exps, RelDataType rowType) {
        return new OLAPProjectRel(getCluster(), traitSet, child, exps, rowType);
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        implementor.visitChild(getInput(), this);

        this.context = implementor.getContext();
        this.hasJoin = context.hasJoin;
        this.afterJoin = context.afterJoin;
        this.afterAggregate = context.afterAggregate;

        this.columnRowType = buildColumnRowType();
    }

    private ColumnRowType buildColumnRowType() {
        List<TblColRef> columns = new ArrayList<TblColRef>();
        List<Set<TblColRef>> sourceColumns = new ArrayList<Set<TblColRef>>();
        OLAPRel olapChild = (OLAPRel) getInput();
        ColumnRowType inputColumnRowType = olapChild.getColumnRowType();
        for (int i = 0; i < this.rewriteProjects.size(); i++) {
            RexNode rex = this.rewriteProjects.get(i);
            RelDataTypeField columnField = this.rowType.getFieldList().get(i);
            String fieldName = columnField.getName();
            Set<TblColRef> sourceCollector = new HashSet<TblColRef>();
            TblColRef column = translateRexNode(rex, inputColumnRowType, fieldName, sourceCollector);
            if (column == null)
                throw new IllegalStateException("No TblColRef found in " + rex);
            columns.add(column);
            sourceColumns.add(sourceCollector);
        }
        return new ColumnRowType(columns, sourceColumns);
    }

    private TblColRef translateRexNode(RexNode rexNode, ColumnRowType inputColumnRowType, String fieldName, Set<TblColRef> sourceCollector) {
        TblColRef column = null;
        if (rexNode instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) rexNode;
            column = translateRexInputRef(inputRef, inputColumnRowType, fieldName, sourceCollector);
        } else if (rexNode instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) rexNode;
            column = translateRexLiteral(literal);
        } else if (rexNode instanceof RexCall) {
            RexCall call = (RexCall) rexNode;
            column = translateRexCall(call, inputColumnRowType, fieldName, sourceCollector);
        } else {
            throw new IllegalStateException("Unsupport RexNode " + rexNode);
        }
        return column;
    }

    private TblColRef translateFirstRexInputRef(RexCall call, ColumnRowType inputColumnRowType, String fieldName, Set<TblColRef> sourceCollector) {
        for (RexNode operand : call.getOperands()) {
            if (operand instanceof RexInputRef) {
                return translateRexInputRef((RexInputRef) operand, inputColumnRowType, fieldName, sourceCollector);
            }
            if (operand instanceof RexCall) {
                TblColRef r = translateFirstRexInputRef((RexCall) operand, inputColumnRowType, fieldName, sourceCollector);
                if (r != null)
                    return r;
            }
        }
        return null;
    }

    private TblColRef translateRexInputRef(RexInputRef inputRef, ColumnRowType inputColumnRowType, String fieldName, Set<TblColRef> sourceCollector) {
        int index = inputRef.getIndex();
        // check it for rewrite count
        if (index < inputColumnRowType.size()) {
            TblColRef column = inputColumnRowType.getColumnByIndex(index);
            if (!column.isInnerColumn() && !this.rewriting && !this.afterAggregate) {
                context.allColumns.add(column);
                sourceCollector.add(column);
            }
            return column;
        } else {
            throw new IllegalStateException("Can't find " + inputRef + " from child columnrowtype " + inputColumnRowType + " with fieldname " + fieldName);
        }
    }

    private TblColRef translateRexLiteral(RexLiteral literal) {
        if (RexLiteral.isNullLiteral(literal)) {
            return TblColRef.newInnerColumn("null", InnerDataTypeEnum.LITERAL);
        } else {
            return TblColRef.newInnerColumn(literal.getValue().toString(), InnerDataTypeEnum.LITERAL);
        }

    }

    private TblColRef translateRexCall(RexCall call, ColumnRowType inputColumnRowType, String fieldName, Set<TblColRef> sourceCollector) {
        SqlOperator operator = call.getOperator();
        if (operator == SqlStdOperatorTable.EXTRACT_DATE) {
            return translateFirstRexInputRef(call, inputColumnRowType, fieldName, sourceCollector);
        } else if (operator instanceof SqlUserDefinedFunction) {
            if (operator.getName().equals("QUARTER")) {
                return translateFirstRexInputRef(call, inputColumnRowType, fieldName, sourceCollector);
            }
        } else if (operator instanceof SqlCaseOperator) {
            for (RexNode operand : call.getOperands()) {
                if (operand instanceof RexInputRef) {
                    RexInputRef inputRef = (RexInputRef) operand;
                    return translateRexInputRef(inputRef, inputColumnRowType, fieldName, sourceCollector);
                }
            }
        }

        for (RexNode operand : call.getOperands()) {
            translateRexNode(operand, inputColumnRowType, fieldName, sourceCollector);
        }
        return TblColRef.newInnerColumn(fieldName, InnerDataTypeEnum.LITERAL);
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        if (getInput() instanceof OLAPFilterRel) {
            // merge project & filter
            OLAPFilterRel filter = (OLAPFilterRel) getInput();
            RelNode inputOfFilter = inputs.get(0).getInput(0);
            RexProgram program = RexProgram.create(inputOfFilter.getRowType(), this.rewriteProjects, filter.getCondition(), this.rowType, getCluster().getRexBuilder());
            return new EnumerableCalc(getCluster(), getCluster().traitSetOf(EnumerableConvention.INSTANCE), //
                    inputOfFilter, program);
        } else {
            // keep project for table scan
            EnumerableRel input = sole(inputs);
            RexProgram program = RexProgram.create(input.getRowType(), this.rewriteProjects, null, this.rowType, getCluster().getRexBuilder());
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
        if (!RewriteImplementor.needRewrite(this.context) || (this.hasJoin && !this.afterJoin) || this.afterAggregate) {
            this.columnRowType = this.buildColumnRowType();
            return;
        }

        // find missed rewrite fields
        int paramIndex = this.rowType.getFieldList().size();
        List<RelDataTypeField> newFieldList = new LinkedList<RelDataTypeField>();
        List<RexNode> newExpList = new LinkedList<RexNode>();
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
            List<RexNode> newProjects = new ArrayList<RexNode>(this.rewriteProjects);
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
}

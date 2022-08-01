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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.schema.OLAPTable;
import org.apache.kylin.query.util.ICutContextStrategy;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.Setter;
import lombok.val;

public class KapProjectRel extends OLAPProjectRel implements KapRel {
    List<RexNode> exps;
    private boolean beforeTopPreCalcJoin = false;
    private Set<OLAPContext> subContexts = Sets.newHashSet();
    @Setter
    private boolean needPushInfoToSubCtx = false;

    public KapProjectRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, List<RexNode> exps,
            RelDataType rowType) {
        super(cluster, traitSet, child, exps, rowType);
        this.exps = exps;
    }

    @Override
    public void implementCutContext(ICutContextStrategy.CutContextImplementor implementor) {
        this.context = null;
        this.columnRowType = null;
        implementor.visitChild(getInput());
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode child, List<RexNode> exps, RelDataType rowType) {
        return new KapProjectRel(getCluster(), traitSet, child, exps, rowType);
    }

    @Override
    public void setContext(OLAPContext context) {
        this.context = context;
        ((KapRel) getInput()).setContext(context);
        subContexts.addAll(ContextUtil.collectSubContext((KapRel) this.getInput()));
    }

    @Override
    public boolean pushRelInfoToContext(OLAPContext context) {
        if (this.context == null && ((KapRel) getInput()).pushRelInfoToContext(context)) {
            this.context = context;
            return true;
        }
        return false;
    }

    @Override
    public void implementContext(OLAPContextImplementor olapContextImplementor, ContextVisitorState state) {
        olapContextImplementor.fixSharedOlapTableScan(this);
        ContextVisitorState tempState = ContextVisitorState.init();
        olapContextImplementor.visitChild(getInput(), this, tempState);
        subContexts.addAll(ContextUtil.collectSubContext((KapRel) this.getInput()));
        if (context == null && subContexts.size() == 1
                && this.getInput() == Lists.newArrayList(this.subContexts).get(0).getTopNode()
                && !(this.getInput() instanceof KapWindowRel)) {
            this.context = Lists.newArrayList(this.subContexts).get(0);
            this.context.setTopNode(this);
        }
        state.merge(tempState);
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        if (this.getPermutation() != null && !isTopProject(implementor.getParentNodeStack()))
            isMerelyPermutation = true;
        // @beforeTopPreCalcJoin refer to this rel is under a preCalcJoin Rel and need not be rewrite. eg.
        //        JOIN
        //       /    \
        //     Proj   TableScan
        //    /
        //  TableScan
        this.beforeTopPreCalcJoin = context != null && context.isHasPreCalcJoin();
        implementor.visitChild(getInput(), this);

        this.columnRowType = buildColumnRowType();
        if (context != null) {
            this.hasJoin = context.isHasJoin();
            this.afterAggregate = context.afterAggregate;
            if (this == context.getTopNode() && !context.isHasAgg())
                KapContext.amendAllColsIfNoAgg(this);
        } else if (this.needPushInfoToSubCtx) {
            updateSubContexts(subContexts);
        }
    }

    private boolean isTopProject(Stack<RelNode> parentNodeStack) {
        val tmpStack = (Stack<RelNode>) parentNodeStack.clone();
        while (!tmpStack.empty()) {
            val parentNode = tmpStack.pop();
            if (parentNode instanceof OLAPToEnumerableConverter)
                return true;

            if (parentNode instanceof OLAPProjectRel)
                return false;
        }

        return false;
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, getInput());
        if (this.context == null) {
            return;
        }
        this.rewriting = true;

        // project before join or is just after OLAPToEnumerableConverter
        if (!RewriteImplementor.needRewrite(this.context) || this.afterAggregate
                || !(this.context.hasPrecalculatedFields())
                || (this.getContext().isHasJoin() && this.beforeTopPreCalcJoin)) {
            this.columnRowType = this.buildColumnRowType();
            return;
        }

        List<RelDataTypeField> newFieldList = Lists.newLinkedList();
        Map<Integer, RelDataTypeField> needReplaceCCFieldList = replaceCcFiledWithOriginInnerCol(newFieldList);
        newFieldList.addAll(rebuildMissPreCalcField());

        // rebuild row type
        if (!newFieldList.isEmpty() && needReplaceCCFieldList.isEmpty()) {
            RelDataTypeFactory.FieldInfoBuilder fieldInfo = getCluster().getTypeFactory().builder();
            fieldInfo.addAll(this.rowType.getFieldList());
            fieldInfo.addAll(newFieldList);
            this.rowType = getCluster().getTypeFactory().createStructType(fieldInfo);
        } else if (!newFieldList.isEmpty()) {
            RelDataTypeFactory.FieldInfoBuilder fieldInfo = getCluster().getTypeFactory().builder();
            List<RelDataTypeField> originfields = Lists.newArrayList(this.rowType.getFieldList());
            for (Map.Entry<Integer, RelDataTypeField> integerRelDataTypeFieldEntry : needReplaceCCFieldList
                    .entrySet()) {
                originfields.set(integerRelDataTypeFieldEntry.getKey(), integerRelDataTypeFieldEntry.getValue());
            }
            fieldInfo.addAll(originfields);
            fieldInfo.addAll(newFieldList);
            this.rowType = getCluster().getTypeFactory().createStructType(fieldInfo);
        }

        // rebuild columns
        this.columnRowType = this.buildColumnRowType();
        this.rewriting = false;
    }

    private void updateSubContexts(Set<OLAPContext> subContexts) {
        if (isMerelyPermutation || this.rewriting || this.afterAggregate)
            return;

        ContextUtil.updateSubContexts(
                this.columnRowType.getSourceColumns().stream().flatMap(Collection::stream).collect(Collectors.toSet()),
                subContexts);
    }

    @Override
    public Set<OLAPContext> getSubContext() {
        return subContexts;
    }

    @Override
    public void setSubContexts(Set<OLAPContext> contexts) {
        this.subContexts = contexts;
    }

    private Map<Integer, RelDataTypeField> replaceCcFiledWithOriginInnerCol(List<RelDataTypeField> newFieldList) {
        Map<Integer, RelDataTypeField> needReplaceCCFieldList = Maps.newHashMap();
        Map<Integer, RexNode> posInTupleToCcCol = Maps.newHashMap();
        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();
        int paramIndex = this.rowType.getFieldList().size();
        for (Map.Entry<TblColRef, TblColRef> originExprToCcCol : this.context.getGroupCCColRewriteMapping()
                .entrySet()) {
            String replaceCCField = originExprToCcCol.getValue().getName();
            int rowIndex = this.columnRowType.getIndexByNameAndByContext(this.context, replaceCCField);
            if (rowIndex >= 0) {
                continue;
            }

            RelDataType ccFieldType = OLAPTable.createSqlType(getCluster().getTypeFactory(),
                    originExprToCcCol.getValue().getType(), true);
            int originExprIndex = findInnerColPosInPrjRelRowType(originExprToCcCol.getKey(), this);
            if (originExprIndex < 0) {
                newFieldList.add(new RelDataTypeFieldImpl(replaceCCField, paramIndex++, ccFieldType));
                int idx = inputColumnRowType.getIndexByNameAndByContext(this.context, replaceCCField);
                RelDataTypeField inputField = getInput().getRowType().getFieldList().get(idx);
                List<RexNode> newRewriteProjects = Lists.newArrayList(this.rewriteProjects);
                newRewriteProjects.add(new RexInputRef(inputField.getIndex(), inputField.getType()));
                this.rewriteProjects = newRewriteProjects;
                continue;
            }
            int ccColInInputIndex = inputColumnRowType.getIndexByNameAndByContext(this.context, replaceCCField);
            if (ccColInInputIndex >= 0) {
                RelDataTypeField newCcFiled = new RelDataTypeFieldImpl(replaceCCField, originExprIndex, ccFieldType);
                needReplaceCCFieldList.put(originExprIndex, newCcFiled);
                RelDataTypeField inputField = getInput().getRowType().getFieldList().get(ccColInInputIndex);
                RexInputRef ccFiledRef = new RexInputRef(inputField.getIndex(), inputField.getType());
                posInTupleToCcCol.put(originExprIndex, ccFiledRef);
            }
        }

        if (!posInTupleToCcCol.isEmpty()) {
            List<RexNode> newProjects = new ArrayList<>(this.rewriteProjects);
            posInTupleToCcCol.forEach(newProjects::set);
            this.rewriteProjects = newProjects;
        }

        return needReplaceCCFieldList;
    }

    private List<RelDataTypeField> rebuildMissPreCalcField() {
        List<RelDataTypeField> newFieldList = Lists.newLinkedList();
        List<RexNode> newExpList = Lists.newArrayList();
        List<RelDataTypeField> inputFieldList = getInput().getRowType().getFieldList();
        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();

        // rebuild origin column
        List<TblColRef> allColumns = this.columnRowType.getAllColumns();
        for (int i = 0; i < this.rewriteProjects.size(); i++) {
            RexNode rexNode = this.rewriteProjects.get(i);
            if (i >= allColumns.size() || !(rexNode instanceof RexInputRef)) {
                newExpList.add(rexNode);
                continue;
            }
            String inputColumnName = inputColumnRowType.getAllColumns().get(((RexInputRef) rexNode).getIndex())
                    .getCanonicalName();
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
        for (Map.Entry<String, RelDataType> rewriteField : this.context.rewriteFields.entrySet()) {
            String rewriteFieldName = rewriteField.getKey();
            int rowIndex = this.columnRowType.getIndexByNameAndByContext(this.context, rewriteFieldName);
            if (rowIndex >= 0) {
                continue;
            }
            int inputIndex = inputColumnRowType.getIndexByNameAndByContext(this.context, rewriteFieldName);
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

    private int findInnerColPosInPrjRelRowType(TblColRef colRef, KapProjectRel rel) {
        for (int i = 0; i < rel.getColumnRowType().getAllColumns().size(); i++) {
            if (colRef.equals(rel.getColumnRowType().getColumnByIndex(i))) {
                return i;
            }
        }
        return -1;
    }
}

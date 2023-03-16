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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.schema.OLAPSchema;
import org.apache.kylin.query.schema.OLAPTable;
import org.apache.kylin.query.engine.KECalciteConfig;
import org.apache.kylin.query.util.ICutContextStrategy;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

/**
 */
public class KapTableScan extends OLAPTableScan implements EnumerableRel, KapRel {

    boolean contextVisited = false; // means whether this TableScan has been visited in context implementor
    private Set<OLAPContext> subContexts = Sets.newHashSet();

    public KapTableScan(RelOptCluster cluster, RelOptTable table, OLAPTable olapTable, int[] fields) {
        super(cluster, table, olapTable, fields);
    }

    @Override
    public void implementCutContext(ICutContextStrategy.CutContextImplementor implementor) {
        // do nothing
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        Preconditions.checkArgument(inputs.isEmpty());
        return new KapTableScan(getCluster(), table, olapTable, fields);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public void setContext(OLAPContext context) {
        this.context = context;
    }

    @Override
    public boolean pushRelInfoToContext(OLAPContext context) {
        return context == this.context;
    }

    @Override
    public void implementContext(OLAPContextImplementor olapContextImplementor, ContextVisitorState state) {
        contextVisited = true;

        final TableDesc sourceTable = this.getOlapTable().getSourceTable();
        state.merge(ContextVisitorState.of(false, true, sourceTable.isIncrementLoading()));

        if (olapContextImplementor.getFirstTableDesc() == null) {
            olapContextImplementor.setFirstTableDesc(sourceTable);
        }
        state.setHasFirstTable(olapContextImplementor.getFirstTableDesc().equals(sourceTable));
    }

    @Override
    public void implementOLAP(OLAPImplementor olapContextImplementor) {
        context.allTableScans.add(this);
        columnRowType = buildColumnRowType();

        if (context.olapSchema == null) {
            OLAPSchema schema = olapTable.getSchema();
            context.olapSchema = schema;
        }
        if (context.firstTableScan == null) {
            context.firstTableScan = this;
        }
        if (needCollectionColumns(olapContextImplementor.getParentNodeStack())) {
            // OLAPToEnumerableConverter on top of table scan, should be a select * from table
            for (TblColRef tblColRef : columnRowType.getAllColumns()) {
                // do not include
                // 1. col with _KY_
                // 2. CC col when exposeComputedColumn config is set to false
                if (!tblColRef.getName().startsWith("_KY_") && !(tblColRef.getColumnDesc().isComputedColumn()
                        && !KECalciteConfig.current().exposeComputedColumn())) {
                    context.allColumns.add(tblColRef);
                }
            }
        }
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        if (context != null) {
            Map<String, RelDataType> rewriteFields = this.context.rewriteFields;
            for (Map.Entry<String, RelDataType> rewriteField : rewriteFields.entrySet()) {
                String fieldName = rewriteField.getKey();
                RelDataTypeField field = rowType.getField(fieldName, true, false);
                if (field != null) {
                    RelDataType fieldType = field.getType();
                    rewriteField.setValue(fieldType);
                }
            }
        }
    }

    /**
     * There're 3 special RelNode in parents stack, OLAPProjectRel, OLAPToEnumerableConverter
     * and OLAPUnionRel. OLAPProjectRel will helps collect required columns but the other two
     * don't. Go through the parent RelNodes from bottom to top, and the first-met special
     * RelNode determines the behavior.
     *      * OLAPProjectRel -> skip column collection
     *      * OLAPToEnumerableConverter and OLAPUnionRel -> require column collection
     */
    @Override
    protected boolean needCollectionColumns(Stack<RelNode> allParents) {
        KapRel topProjParent = null;
        for (RelNode tempParent : allParents) {
            if (tempParent instanceof KapOLAPToEnumerableConverter) {
                continue;
            }
            if (!(tempParent instanceof KapRel)) {
                break;
            }
            KapRel parent = (KapRel) tempParent;

            if (topProjParent == null && parent instanceof OLAPProjectRel
                    && !((OLAPProjectRel) parent).isMerelyPermutation()) {
                topProjParent = parent;
            }

            if (parent instanceof OLAPToEnumerableConverter || parent instanceof OLAPUnionRel
                    || parent instanceof KapMinusRel || parent instanceof KapAggregateRel) {
                topProjParent = null;
            }
        }

        if (topProjParent != null) {
            ((KapProjectRel) topProjParent).setNeedPushInfoToSubCtx(true);
        }
        return topProjParent == null;
    }

    @Override
    public Set<OLAPContext> getSubContext() {
        return subContexts;
    }

    @Override
    public void setSubContexts(Set<OLAPContext> contexts) {
        this.subContexts = contexts;
    }
}

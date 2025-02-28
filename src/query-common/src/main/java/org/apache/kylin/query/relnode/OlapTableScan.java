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
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.engine.KylinConnectionConfig;
import org.apache.kylin.query.schema.OlapTable;
import org.apache.kylin.query.util.ICutContextStrategy;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@Getter
public class OlapTableScan extends TableScan implements EnumerableRel, OlapRel {

    private OlapTable olapTable;
    private String tableName;
    private int[] fields;
    private ColumnRowType columnRowType;
    @Getter(AccessLevel.PRIVATE)
    private KylinConfig kylinConfig;
    @Setter
    private OlapContext context;
    @Setter
    private Set<OlapContext> subContexts = Sets.newHashSet();
    private String alias;
    private String backupAlias;
    /** Whether this TableScan has been visited in context implementor */
    private boolean contextVisited = false;

    public OlapTableScan(RelOptCluster cluster, RelOptTable table, OlapTable olapTable, int[] fields) {
        super(cluster, cluster.traitSetOf(CONVENTION), table);
        this.olapTable = olapTable;
        this.fields = fields;
        this.tableName = olapTable.getTableName();
        this.rowType = getRowType();
        this.kylinConfig = KylinConfig.getInstanceFromEnv();
    }

    private static RelOptCluster emptyCluster() {
        VolcanoPlanner emptyPlanner = new VolcanoPlanner();
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        return RelOptCluster.create(emptyPlanner, new RexBuilder(typeFactory));
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        Preconditions.checkArgument(inputs.isEmpty());
        return new OlapTableScan(getCluster(), table, olapTable, fields);
    }

    @Override
    public RelDataType deriveRowType() {
        final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
        List<RelDataTypeField> list = Arrays.stream(this.fields) //
                .mapToObj(fieldList::get).collect(Collectors.toList());
        return getCluster().getTypeFactory().createStructType(list);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(OlapRel.OLAP_COST_FACTOR);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {

        return super.explainTerms(pw).item("ctx", displayCtxId(context)) //
                .item("fields", Primitive.asList(fields));
    }

    @Override
    public void implementOlap(OlapImpl olapImpl) {
        context.getAllTableScans().add(this);
        columnRowType = buildColumnRowType();

        if (context.getOlapSchema() == null) {
            context.setOlapSchema(olapTable.getSchema());
        }
        if (context.getFirstTableScan() == null) {
            context.setFirstTableScan(this);
        }
        if (needCollectionColumns(olapImpl.getParentNodeStack())) {
            // OlapToEnumerableConverter on top of table scan, should be a select * from table
            for (TblColRef tblColRef : columnRowType.getAllColumns()) {
                // do not include
                // 1. col with _KY_
                // 2. CC col when exposeComputedColumn config is set to false
                if (!tblColRef.getName().startsWith("_KY_") && !(tblColRef.getColumnDesc().isComputedColumn()
                        && !KylinConnectionConfig.current().exposeComputedColumn())) {
                    context.getAllColumns().add(tblColRef);
                }
            }
        }
    }

    /**
     * There are 3 special RelNode in parents stack, OlapProjectRel, OlapToEnumerableConverter
     * and OlapUnionRel. OlapProjectRel will helps collect required columns but the other two
     * don't. Go through the parent RelNodes from bottom to top, and the first-met special
     * RelNode determines the behavior.
     *      * OlapProjectRel -> skip column collection
     *      * OlapToEnumerableConverter and OlapUnionRel -> require column collection
     */
    private boolean needCollectionColumns(Deque<RelNode> allParents) {
        OlapRel topProjParent = null;
        for (RelNode tempParent : allParents) {
            if (tempParent instanceof OlapToEnumerableConverter) {
                continue;
            }
            if (!(tempParent instanceof OlapRel)) {
                break;
            }
            OlapRel parent = (OlapRel) tempParent;
            if (parent instanceof OlapSortRel) {
                ((OlapSortRel) parent).setNeedPushToSubCtx(true);
            }

            if (topProjParent == null && parent instanceof OlapProjectRel
                    && !((OlapProjectRel) parent).isMerelyPermutation()) {
                topProjParent = parent;
            }

            if (parent instanceof OlapUnionRel || parent instanceof OlapMinusRel
                    || parent instanceof OlapAggregateRel) {
                topProjParent = null;
            }
        }

        if (topProjParent != null) {
            ((OlapProjectRel) topProjParent).setNeedPushInfoToSubCtx(true);
        }
        return topProjParent == null;
    }

    private ColumnRowType buildColumnRowType() {
        this.alias = ("T_" + context.getAllTableScans().size() + "_"
                + Integer.toHexString(System.identityHashCode(this))).toUpperCase(Locale.ROOT);
        TableRef tableRef = TblColRef.tableForUnknownModel(this.alias, olapTable.getSourceTable());

        List<TblColRef> columns = new ArrayList<>();
        for (ColumnDesc sourceColumn : olapTable.getSourceColumns()) {
            TblColRef colRef = TblColRef.columnForUnknownModel(tableRef, sourceColumn);
            columns.add(colRef);
        }

        if (columns.size() != rowType.getFieldCount()) {
            throw new IllegalStateException("RowType=" + rowType.getFieldCount() //
                    + ", ColumnRowType=" + columns.size());
        }
        return new ColumnRowType(columns);
    }

    public TableRef getTableRef() {
        return columnRowType.getColumnByIndex(0).getTableRef();
    }

    public TblColRef makeRewriteColumn(String name) {
        return getTableRef().makeFakeColumn(name);
    }

    public void fixColumnRowTypeWithModel(NDataModel model, Map<String, String> aliasMap) {
        String newAlias = aliasMap.get(this.alias);
        for (TblColRef col : columnRowType.getAllColumns()) {
            TblColRef.fixUnknownModel(model, newAlias, col);
        }

        this.backupAlias = this.alias;
        this.alias = newAlias;
    }

    public void unfixColumnRowTypeWithModel() {
        this.alias = this.backupAlias;
        this.backupAlias = null;

        for (TblColRef col : columnRowType.getAllColumns()) {
            TblColRef.unfixUnknownModel(col);
        }
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {

        return this;
    }

    /**
     * belongs to legacy "calcite query engine" (compared to current "sparder query engine"), pay less attention
     */
    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        String execFunction = context.genExecFunc(this);
        PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), JavaRowFormat.ARRAY, false);
        MethodCallExpression exprCall = Expressions.call(table.getExpression(OlapTable.class), execFunction,
                implementor.getRootExpression(), Expressions.constant(context.getId()));
        return implementor.result(physType, Blocks.toBlock(exprCall));
    }

    public void setColumnRowType(ColumnRowType columnRowType) {
        this.columnRowType = columnRowType;
    }

    @Override
    public void implementRewrite(RewriteImpl rewriteImpl) {
        if (context != null) {
            this.context.getRewriteFields().entrySet().forEach(rewriteField -> {
                String fieldName = rewriteField.getKey();
                RelDataTypeField field = rowType.getField(fieldName, true, false);
                if (field != null) {
                    RelDataType fieldType = field.getType();
                    rewriteField.setValue(fieldType);
                }
            });
        }
    }

    @Override
    public boolean hasSubQuery() {
        return false;
    }

    @Override
    public RelTraitSet replaceTraitSet(RelTrait trait) {
        RelTraitSet oldTraitSet = this.traitSet;
        this.traitSet = this.traitSet.replace(trait);
        return oldTraitSet;
    }

    public OlapTableScan cleanRelOptCluster() {
        OlapTableScan tableScan = new OlapTableScan(emptyCluster(), this.table, this.olapTable, this.fields);
        tableScan.getCluster().getPlanner().clear();
        tableScan.columnRowType = this.columnRowType;
        tableScan.olapTable = this.olapTable;
        tableScan.fields = fields;
        tableScan.tableName = this.tableName;
        tableScan.context = this.context;
        tableScan.kylinConfig = this.kylinConfig;
        tableScan.digest = this.digest;
        tableScan.alias = this.alias;
        return tableScan;
    }

    @Override
    public void implementCutContext(ICutContextStrategy.ContextCutImpl contextCutImpl) {
        // do nothing
    }

    @Override
    public boolean pushRelInfoToContext(OlapContext context) {
        return context == this.context;
    }

    @Override
    public void implementContext(ContextImpl contextImpl, ContextVisitorState state) {
        contextVisited = true;

        final TableDesc sourceTable = this.getOlapTable().getSourceTable();
        state.merge(ContextVisitorState.of(false, true, sourceTable.isIncrementLoading()));

        if (contextImpl.getFirstTableDesc() == null) {
            contextImpl.setFirstTableDesc(sourceTable);
        }
        state.setHasFirstTable(contextImpl.getFirstTableDesc().equals(sourceTable));
    }
}

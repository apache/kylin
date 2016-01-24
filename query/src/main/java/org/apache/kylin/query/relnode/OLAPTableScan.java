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
import java.util.List;
import java.util.Map;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
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
import org.apache.calcite.plan.volcano.AbstractConverter.ExpandConversionRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SortJoinTransposeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.optrule.OLAPAggregateRule;
import org.apache.kylin.query.optrule.OLAPFilterRule;
import org.apache.kylin.query.optrule.OLAPJoinRule;
import org.apache.kylin.query.optrule.OLAPLimitRule;
import org.apache.kylin.query.optrule.OLAPProjectRule;
import org.apache.kylin.query.optrule.OLAPSortRule;
import org.apache.kylin.query.optrule.OLAPToEnumerableConverterRule;
import org.apache.kylin.query.schema.OLAPSchema;
import org.apache.kylin.query.schema.OLAPTable;

import com.google.common.base.Preconditions;

/**
 */
public class OLAPTableScan extends TableScan implements OLAPRel, EnumerableRel {

    private final OLAPTable olapTable;
    private final String tableName;
    private final int[] fields;
    private ColumnRowType columnRowType;
    private OLAPContext context;

    public OLAPTableScan(RelOptCluster cluster, RelOptTable table, OLAPTable olapTable, int[] fields) {
        super(cluster, cluster.traitSetOf(OLAPRel.CONVENTION), table);
        this.olapTable = olapTable;
        this.fields = fields;
        this.tableName = olapTable.getTableName();
        this.rowType = getRowType();
    }

    public OLAPTable getOlapTable() {
        return olapTable;
    }

    public String getTableName() {
        return tableName;
    }

    public int[] getFields() {
        return fields;
    }

    @Override
    public OLAPContext getContext() {
        return context;
    }

    void overrideContext(OLAPContext context) {
        this.context = context;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        Preconditions.checkArgument(inputs.isEmpty());
        return new OLAPTableScan(getCluster(), table, olapTable, fields);
    }

    @Override
    public void register(RelOptPlanner planner) {
        // force clear the query context before traversal relational operators
        OLAPContext.clearThreadLocalContexts();

        // register OLAP rules
        planner.addRule(OLAPToEnumerableConverterRule.INSTANCE);
        planner.addRule(OLAPFilterRule.INSTANCE);
        planner.addRule(OLAPProjectRule.INSTANCE);
        planner.addRule(OLAPAggregateRule.INSTANCE);
        planner.addRule(OLAPJoinRule.INSTANCE);
        planner.addRule(OLAPLimitRule.INSTANCE);
        planner.addRule(OLAPSortRule.INSTANCE);

        // CalcitePrepareImpl.CONSTANT_REDUCTION_RULES
        planner.addRule(ReduceExpressionsRule.PROJECT_INSTANCE);
        planner.addRule(ReduceExpressionsRule.FILTER_INSTANCE);
        planner.addRule(ReduceExpressionsRule.CALC_INSTANCE);
        planner.addRule(ReduceExpressionsRule.JOIN_INSTANCE);
        // the ValuesReduceRule breaks query test somehow...
        //        planner.addRule(ValuesReduceRule.FILTER_INSTANCE);
        //        planner.addRule(ValuesReduceRule.PROJECT_FILTER_INSTANCE);
        //        planner.addRule(ValuesReduceRule.PROJECT_INSTANCE);

        // since join is the entry point, we can't push filter past join
        planner.removeRule(FilterJoinRule.FILTER_ON_JOIN);
        planner.removeRule(FilterJoinRule.JOIN);

        // since we don't have statistic of table, the optimization of join is too cost
        planner.removeRule(JoinCommuteRule.INSTANCE);
        planner.removeRule(JoinPushThroughJoinRule.LEFT);
        planner.removeRule(JoinPushThroughJoinRule.RIGHT);

        // keep tree structure like filter -> aggregation -> project -> join/table scan, implementOLAP() rely on this tree pattern
        planner.removeRule(AggregateJoinTransposeRule.INSTANCE);
        planner.removeRule(AggregateProjectMergeRule.INSTANCE);
        planner.removeRule(FilterProjectTransposeRule.INSTANCE);
        planner.removeRule(SortJoinTransposeRule.INSTANCE);
        planner.removeRule(JoinPushExpressionsRule.INSTANCE);
        // distinct count will be split into a separated query that is joined with the left query
        planner.removeRule(AggregateExpandDistinctAggregatesRule.INSTANCE);

        // see Dec 26th email @ http://mail-archives.apache.org/mod_mbox/calcite-dev/201412.mbox/browser
        planner.removeRule(ExpandConversionRule.INSTANCE);
    }

    @Override
    public RelDataType deriveRowType() {
        final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
        final RelDataTypeFactory.FieldInfoBuilder builder = getCluster().getTypeFactory().builder();
        for (int field : fields) {
            builder.add(fieldList.get(field));
        }
        return getCluster().getTypeFactory().createStructType(builder);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.05);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("fields", Primitive.asList(fields));
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        // create context in case of non-join
        if (implementor.getContext() == null || !(implementor.getParentNode() instanceof OLAPJoinRel)) {
            implementor.allocateContext();
        }
        columnRowType = buildColumnRowType();
        context = implementor.getContext();

        if (context.olapSchema == null) {
            OLAPSchema schema = olapTable.getSchema();
            context.olapSchema = schema;
            context.storageContext.setConnUrl(schema.getStorageUrl());
        }

        if (context.firstTableScan == null) {
            context.firstTableScan = this;
        }

        context.olapRowType = rowType;
    }

    private ColumnRowType buildColumnRowType() {
        List<TblColRef> columns = new ArrayList<TblColRef>();
        for (ColumnDesc sourceColumn : olapTable.getExposedColumns()) {
            TblColRef colRef = new TblColRef(sourceColumn);
            columns.add(colRef);
        }
        return new ColumnRowType(columns);
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        return this;
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), this.rowType, pref.preferArray());

        String execFunction = genExecFunc();

        MethodCallExpression exprCall = Expressions.call(table.getExpression(OLAPTable.class), execFunction, implementor.getRootExpression(), Expressions.constant(context.id));
        return implementor.result(physType, Blocks.toBlock(exprCall));
    }

    private String genExecFunc() {
        // if the table to scan is not the fact table of cube, then it's a lookup table
        if (context.hasJoin == false && tableName.equalsIgnoreCase(context.realization.getFactTable()) == false) {
            return "executeLookupTableQuery";
        } else {
            return "executeIndexQuery";
        }

    }

    @Override
    public ColumnRowType getColumnRowType() {
        return columnRowType;
    }

    /**
     * Because OLAPTableScan is reused for the same table, we can't use
     * this.context and have to use parent context
     */
    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        Map<String, RelDataType> rewriteFields = this.context.rewriteFields;
        if (implementor.getParentContext() != null) {
            rewriteFields = implementor.getParentContext().rewriteFields;
        }

        for (Map.Entry<String, RelDataType> rewriteField : rewriteFields.entrySet()) {
            String fieldName = rewriteField.getKey();
            RelDataTypeField field = rowType.getField(fieldName, true, false);
            if (field != null) {
                RelDataType fieldType = field.getType();
                rewriteField.setValue(fieldType);
            }
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
}

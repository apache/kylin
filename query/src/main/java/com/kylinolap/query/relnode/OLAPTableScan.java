/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.query.relnode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.hydromatic.linq4j.expressions.Blocks;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.expressions.Primitive;
import net.hydromatic.optiq.rules.java.EnumerableRel;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;
import net.hydromatic.optiq.rules.java.PhysType;
import net.hydromatic.optiq.rules.java.PhysTypeImpl;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;

import com.google.common.base.Preconditions;
import com.kylinolap.metadata.model.cube.TblColRef;
import com.kylinolap.metadata.model.schema.ColumnDesc;
import com.kylinolap.query.optrule.OLAPAggregateRule;
import com.kylinolap.query.optrule.OLAPFilterRule;
import com.kylinolap.query.optrule.OLAPJoinRule;
import com.kylinolap.query.optrule.OLAPLimitRule;
import com.kylinolap.query.optrule.OLAPProjectRule;
import com.kylinolap.query.optrule.OLAPSortRule;
import com.kylinolap.query.optrule.OLAPToEnumerableConverterRule;
import com.kylinolap.query.schema.OLAPSchema;
import com.kylinolap.query.schema.OLAPTable;

/**
 * @author xjiang
 */
public class OLAPTableScan extends TableAccessRelBase implements OLAPRel, EnumerableRel {

    private final OLAPTable olapTable;
    private final String cubeTable;
    private final int[] fields;
    private ColumnRowType columnRowType;
    private OLAPContext context;

    public OLAPTableScan(RelOptCluster cluster, RelOptTable table, OLAPTable olapTable, int[] fields) {
        super(cluster, cluster.traitSetOf(OLAPRel.CONVENTION), table);
        this.olapTable = olapTable;
        this.fields = fields;
        this.cubeTable = olapTable.getTableName();
        this.rowType = getRowType();
    }

    public OLAPTable getOlapTable() {
        return olapTable;
    }

    public String getCubeTable() {
        return cubeTable;
    }

    public int[] getFields() {
        return fields;
    }

    @Override
    public OLAPContext getContext() {
        return context;
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

        // since join is the entry point, we can't push filter past join
        planner.removeRule(PushFilterPastJoinRule.FILTER_ON_JOIN);
        planner.removeRule(PushFilterPastJoinRule.JOIN);

        // TODO : since we don't have statistic of table, the optimization of join is too cost
        planner.removeRule(SwapJoinRule.INSTANCE);
        planner.removeRule(PushJoinThroughJoinRule.LEFT);
        planner.removeRule(PushJoinThroughJoinRule.RIGHT);

        // for columns in having clause will enable table scan filter rule
        // cause kylin does not depend on MPP
        planner.removeRule(PushFilterPastProjectRule.INSTANCE);
        // distinct count will be split into a separated query that is joined with the left query
        planner.removeRule(RemoveDistinctAggregateRule.INSTANCE);
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
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return super.computeSelfCost(planner).multiplyBy(.05);
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
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        if (!(implementor instanceof JavaImplementor))
            throw new IllegalStateException("implementor is not JavaImplementor");
        JavaImplementor javaImplementor = (JavaImplementor) implementor;

        int ctxId = this.context.id;
        if (javaImplementor.getParentContext() != null) {
            ctxId = javaImplementor.getParentContext().id;
        }

        PhysType physType = PhysTypeImpl.of(javaImplementor.getTypeFactory(), this.rowType, pref.preferArray());

        String execFunction = genExecFunc();

        return javaImplementor.result(physType, Blocks.toBlock(Expressions.call(table.getExpression(OLAPTable.class), execFunction, javaImplementor.getRootExpression(), Expressions.constant(ctxId))));
    }

    private String genExecFunc() {
        // if the table to scan is not the fact table of cube, then it's a
        // lookup table
        if (context.hasJoin == false && cubeTable.equals(context.cubeDesc.getFactTable()) == false) {
            return "executeLookupTableQuery";
        } else {
            return "executeCubeQuery";
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
            RelDataTypeField field = rowType.getField(fieldName, true);
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

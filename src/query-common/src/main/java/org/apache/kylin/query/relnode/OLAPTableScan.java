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
import java.util.Locale;
import java.util.Map;
import java.util.Stack;

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
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.schema.OLAPSchema;
import org.apache.kylin.query.schema.OLAPTable;
import org.apache.kylin.metadata.model.NDataModel;

import com.google.common.base.Preconditions;

/**
 */
public class OLAPTableScan extends TableScan implements OLAPRel, EnumerableRel {

    protected OLAPTable olapTable;
    protected String tableName;
    protected int[] fields;
    protected ColumnRowType columnRowType;
    protected OLAPContext context;
    protected KylinConfig kylinConfig;
    private String alias;
    private String backupAlias;

    public OLAPTableScan(RelOptCluster cluster, RelOptTable table, OLAPTable olapTable, int[] fields) {
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

    public OLAPTable getOlapTable() {
        return olapTable;
    }

    public String getTableName() {
        return tableName;
    }

    public int[] getFields() {
        return fields;
    }

    public String getBackupAlias() {
        return backupAlias;
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

        return super.explainTerms(pw)
                .item("ctx", context == null ? "" : String.valueOf(context.id) + "@" + context.realization)
                .item("fields", Primitive.asList(fields));
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        Preconditions.checkState(columnRowType == null, "OLAPTableScan MUST NOT be shared by more than one prent");

        // create context in case of non-join
        if (implementor.getContext() == null || !(implementor.getParentNode() instanceof OLAPJoinRel)
                || implementor.isNewOLAPContextRequired()) {
            implementor.allocateContext();
        }

        context = implementor.getContext();
        context.allTableScans.add(this);
        columnRowType = buildColumnRowType();

        if (context.olapSchema == null) {
            OLAPSchema schema = olapTable.getSchema();
            context.olapSchema = schema;
        }

        if (context.firstTableScan == null) {
            context.firstTableScan = this;
        }

        if (needCollectionColumns(implementor.getParentNodeStack())) {
            // OLAPToEnumerableConverter on top of table scan, should be a select * from table
            for (TblColRef tblColRef : columnRowType.getAllColumns()) {
                if (!tblColRef.getName().startsWith("_KY_")) {
                    context.allColumns.add(tblColRef);
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
    protected boolean needCollectionColumns(Stack<RelNode> allParents) {
        int index = allParents.size() - 1;

        while (index >= 0) {
            RelNode parent = allParents.get(index);
            if (parent instanceof OLAPProjectRel) {
                return false;
            }

            if (parent instanceof OLAPToEnumerableConverter || parent instanceof OLAPUnionRel) {
                return true;
            }

            OLAPRel olapParent = (OLAPRel) allParents.get(index);
            if (olapParent.getContext() != null && olapParent.getContext() != this.context) {
                // if the whole context has not projection, let table scan take care of itself
                break;
            }
            index--;
        }

        return true;
    }

    public String getAlias() {
        return alias;
    }

    protected ColumnRowType buildColumnRowType() {
        this.alias = ("T_" + context.allTableScans.size() + "_" + Integer.toHexString(System.identityHashCode(this)))
                .toUpperCase(Locale.ROOT);
        TableRef tableRef = TblColRef.tableForUnknownModel(this.alias, olapTable.getSourceTable());

        List<TblColRef> columns = new ArrayList<>();
        for (ColumnDesc sourceColumn : olapTable.getSourceColumns()) {
            TblColRef colRef = TblColRef.columnForUnknownModel(tableRef, sourceColumn);
            columns.add(colRef);
        }

        if (columns.size() != rowType.getFieldCount()) {
            throw new IllegalStateException("RowType=" + rowType.getFieldCount() + ", ColumnRowType=" + columns.size());
        }
        return new ColumnRowType(columns);
    }

    public TableRef getTableRef() {
        return columnRowType.getColumnByIndex(0).getTableRef();
    }

    @SuppressWarnings("deprecation")
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
        context.setReturnTupleInfo(rowType, columnRowType);
        String execFunction = genExecFunc();

        PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), JavaRowFormat.ARRAY, false);
        MethodCallExpression exprCall = Expressions.call(table.getExpression(OLAPTable.class), execFunction,
                implementor.getRootExpression(), Expressions.constant(context.id));
        return implementor.result(physType, Blocks.toBlock(exprCall));
    }

    public String genExecFunc() {
        context.setReturnTupleInfo(rowType, columnRowType);
        if (context.isConstantQueryWithAggregations())
            return "executeSimpleAggregationQuery";
        // if the table to scan is not the fact table of cube, then it's a lookup table,
        // TODO: this is not right!
        if (context.realization.getModel().isLookupTable(tableName)) {
            return "executeLookupTableQuery";
        } else {
            return "executeOLAPQuery";
        }

    }

    @Override
    public ColumnRowType getColumnRowType() {
        return columnRowType;
    }

    public void setColumnRowType(ColumnRowType columnRowType) {
        this.columnRowType = columnRowType;
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
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

    public OLAPTableScan cleanRelOptCluster() {
        OLAPTableScan tableScan = new OLAPTableScan(emptyCluster(), this.table, this.olapTable, this.fields);
        tableScan.getCluster().getPlanner().clear();
        tableScan.columnRowType = this.columnRowType;
        tableScan.olapTable = this.olapTable;
        tableScan.fields = fields;
        tableScan.tableName = this.tableName;
        tableScan.context = this.context;
        tableScan.kylinConfig = this.kylinConfig;
        tableScan.digest = this.digest;
        tableScan.id = this.id;
        tableScan.alias = this.alias;
        return tableScan;
    }
}

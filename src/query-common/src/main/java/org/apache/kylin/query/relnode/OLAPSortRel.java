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

import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;

import com.google.common.base.Preconditions;

/**
 */
public class OLAPSortRel extends Sort implements OLAPRel {

    protected ColumnRowType columnRowType;
    protected OLAPContext context;

    public OLAPSortRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RelCollation collation,
            RexNode offset, RexNode fetch) {
        super(cluster, traitSet, child, collation, offset, fetch);
        Preconditions.checkArgument(getConvention() == CONVENTION);
        Preconditions.checkArgument(getConvention() == child.getConvention());
    }

    @Override
    public OLAPSortRel copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset,
            RexNode fetch) {
        return new OLAPSortRel(getCluster(), traitSet, newInput, newCollation, offset, fetch);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.05);
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        implementor.fixSharedOlapTableScan(this);
        implementor.visitChild(getInput(), this);

        this.context = implementor.getContext();
        this.columnRowType = buildColumnRowType();

        for (RelFieldCollation fieldCollation : this.collation.getFieldCollations()) {
            int index = fieldCollation.getFieldIndex();
            SQLDigest.OrderEnum order = getOrderEnum(fieldCollation.getDirection());
            OLAPRel olapChild = (OLAPRel) this.getInput();
            TblColRef orderCol = olapChild.getColumnRowType().getAllColumns().get(index);
            this.context.addSort(orderCol, order);
        }
    }

    protected ColumnRowType buildColumnRowType() {
        return ((OLAPRel) getInput()).getColumnRowType();
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, getInput());

        // No need to rewrite "order by" applied on non-olap context.
        // Occurs in sub-query like "select ... from (...) inner join (...) order by ..."
        if (this.context.realization == null)
            return;

        this.rowType = this.deriveRowType();
        this.columnRowType = buildColumnRowType();
    }

    protected SQLDigest.OrderEnum getOrderEnum(RelFieldCollation.Direction direction) {
        if (direction == RelFieldCollation.Direction.DESCENDING) {
            return SQLDigest.OrderEnum.DESCENDING;
        } else {
            return SQLDigest.OrderEnum.ASCENDING;
        }
    }

    @SuppressWarnings("unused")
    MeasureDesc findMeasure(TblColRef col) {
        for (MeasureDesc measure : this.context.realization.getMeasures()) {
            if (col.getName().equals(measure.getFunction().getRewriteFieldName())) {
                return measure;
            }
        }
        return null;
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        return new EnumerableSort(getCluster(),
                getCluster().traitSetOf(EnumerableConvention.INSTANCE).replace(collation), //
                sole(inputs), collation, offset, fetch);
    }

    @Override
    public OLAPContext getContext() {
        return context;
    }

    @Override
    public ColumnRowType getColumnRowType() {
        return columnRowType;
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

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("ctx",
                context == null ? "" : String.valueOf(context.id) + "@" + context.realization);
    }
}

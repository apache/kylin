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

import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.EnumerableRel;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;
import net.hydromatic.optiq.rules.java.JavaRules.EnumerableSortRel;

import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

import com.google.common.base.Preconditions;
import com.kylinolap.cube.model.MeasureDesc;
import com.kylinolap.metadata.model.realization.TblColRef;
import com.kylinolap.storage.StorageContext;

/**
 * @author xjiang
 * 
 */
public class OLAPSortRel extends SortRel implements EnumerableRel, OLAPRel {

    private ColumnRowType columnRowType;
    private OLAPContext context;

    public OLAPSortRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
        super(cluster, traitSet, child, collation, offset, fetch);
        Preconditions.checkArgument(getConvention() == OLAPRel.CONVENTION);
        Preconditions.checkArgument(getConvention() == child.getConvention());
    }

    @Override
    public OLAPSortRel copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch) {
        return new OLAPSortRel(getCluster(), traitSet, newInput, newCollation, offset, fetch);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return super.computeSelfCost(planner).multiplyBy(.05);
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        implementor.visitChild(getChild(), this);

        this.context = implementor.getContext();
        this.columnRowType = buildColumnRowType();
    }

    private ColumnRowType buildColumnRowType() {
        OLAPRel olapChild = (OLAPRel) getChild();
        ColumnRowType inputColumnRowType = olapChild.getColumnRowType();
        return inputColumnRowType;
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, getChild());

        for (RelFieldCollation fieldCollation : this.collation.getFieldCollations()) {
            int index = fieldCollation.getFieldIndex();
            StorageContext.OrderEnum order = getOrderEnum(fieldCollation.getDirection());
            OLAPRel olapChild = (OLAPRel) this.getChild();
            TblColRef orderCol = olapChild.getColumnRowType().getAllColumns().get(index);
            MeasureDesc measure = findMeasure(orderCol);
            if (measure != null) {
                this.context.storageContext.addSort(measure, order);
            }
            this.context.storageContext.markSort();
        }

        this.rowType = this.deriveRowType();
        this.columnRowType = buildColumnRowType();
    }

    private StorageContext.OrderEnum getOrderEnum(RelFieldCollation.Direction direction) {
        if (direction == RelFieldCollation.Direction.DESCENDING) {
            return StorageContext.OrderEnum.DESCENDING;
        } else {
            return StorageContext.OrderEnum.ASCENDING;
        }
    }

    private MeasureDesc findMeasure(TblColRef col) {
        for (MeasureDesc measure : this.context.cubeDesc.getMeasures()) {
            if (col.getName().equals(measure.getFunction().getRewriteFieldName())) {
                return measure;
            }
        }
        return null;
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        OLAPRel childRel = (OLAPRel) getChild();
        childRel.replaceTraitSet(EnumerableConvention.INSTANCE);

        EnumerableSortRel enumSort = new EnumerableSortRel(getCluster(), getCluster().traitSetOf(EnumerableConvention.INSTANCE, collation), getChild(), collation, offset, fetch);

        Result res = enumSort.implement(implementor, pref);

        childRel.replaceTraitSet(OLAPRel.CONVENTION);

        return res;
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
        OLAPRel olapChild = (OLAPRel) getChild();
        return olapChild.hasSubQuery();
    }

    @Override
    public RelTraitSet replaceTraitSet(RelTrait trait) {
        RelTraitSet oldTraitSet = this.traitSet;
        this.traitSet = this.traitSet.replace(trait);
        return oldTraitSet;
    }

}

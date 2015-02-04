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

import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.EnumerableRel;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;
import net.hydromatic.optiq.rules.java.JavaRules.EnumerableLimitRel;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;

import com.google.common.base.Preconditions;

/**
 * 
 * @author xjiang
 * 
 */
public class OLAPLimitRel extends SingleRel implements OLAPRel, EnumerableRel {

    private final RexNode localOffset; // avoid same name in parent class
    private final RexNode localFetch; // avoid same name in parent class
    private ColumnRowType columnRowType;
    private OLAPContext context;

    public OLAPLimitRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode offset, RexNode fetch) {
        super(cluster, traitSet, child);
        Preconditions.checkArgument(getConvention() == CONVENTION);
        Preconditions.checkArgument(getConvention() == child.getConvention());
        this.localOffset = offset;
        this.localFetch = fetch;
    }

    @Override
    public OLAPLimitRel copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OLAPLimitRel(getCluster(), traitSet, sole(inputs), localOffset, localFetch);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return super.computeSelfCost(planner).multiplyBy(.05);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).itemIf("offset", localOffset, localOffset != null).itemIf("fetch", localFetch, localFetch != null);
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        implementor.visitChild(getChild(), this);

        this.columnRowType = buildColumnRowType();

        this.context = implementor.getContext();
        Number limitValue = (Number) (((RexLiteral) localFetch).getValue());
        int limit = limitValue.intValue();
        this.context.storageContext.setLimit(limit);
    }

    private ColumnRowType buildColumnRowType() {
        OLAPRel olapChild = (OLAPRel) getChild();
        ColumnRowType inputColumnRowType = olapChild.getColumnRowType();
        return inputColumnRowType;
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, getChild());

        this.rowType = this.deriveRowType();
        this.columnRowType = buildColumnRowType();
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        OLAPRel childRel = (OLAPRel) getChild();
        childRel.replaceTraitSet(EnumerableConvention.INSTANCE);

        EnumerableLimitRel enumLimit = new EnumerableLimitRel(getCluster(), getCluster().traitSetOf(EnumerableConvention.INSTANCE), getChild(), localOffset, localFetch);
        Result res = enumLimit.implement(implementor, pref);

        childRel.replaceTraitSet(CONVENTION);

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

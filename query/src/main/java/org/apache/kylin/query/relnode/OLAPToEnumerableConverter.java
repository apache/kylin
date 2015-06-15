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

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.routing.NoRealizationFoundException;
import org.apache.kylin.query.routing.QueryRouter;
import org.apache.kylin.query.schema.OLAPTable;

/**
 */
public class OLAPToEnumerableConverter extends ConverterImpl implements EnumerableRel {

    public OLAPToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OLAPToEnumerableConverter(getCluster(), traitSet, sole(inputs));
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return super.computeSelfCost(planner).multiplyBy(.05);
    }

    @Override
    public Result implement(EnumerableRelImplementor enumImplementor, Prefer pref) {
        // post-order travel children
        OLAPRel.OLAPImplementor olapImplementor = new OLAPRel.OLAPImplementor();
        olapImplementor.visitChild(getInput(), this);

        // find cube from olap context
        try {
            for (OLAPContext context : OLAPContext.getThreadLocalContexts()) {
                IRealization realization = QueryRouter.selectRealization(context);
                context.realization = realization;
            }
        } catch (NoRealizationFoundException e) {
            OLAPContext ctx0 = (OLAPContext) OLAPContext.getThreadLocalContexts().toArray()[0];
            if (ctx0 != null && ctx0.olapSchema.hasStarSchemaUrl()) {
                // generate hive result
                return buildHiveResult(enumImplementor, pref, ctx0);
            } else {
                throw e;
            }
        }

        // rewrite query if necessary
        OLAPRel.RewriteImplementor rewriteImplementor = new OLAPRel.RewriteImplementor();
        rewriteImplementor.visitChild(this, getInput());

        // implement as EnumerableRel
        OLAPRel.JavaImplementor impl = new OLAPRel.JavaImplementor(enumImplementor);
        EnumerableRel inputAsEnum = impl.createEnumerable((OLAPRel) getInput());
        this.replaceInput(0, inputAsEnum);
        
        if (System.getProperty("calcite.debug") != null) {
            String dumpPlan = RelOptUtil.dumpPlan("", this, false, SqlExplainLevel.DIGEST_ATTRIBUTES);
            System.out.println("EXECUTION PLAN AFTER REWRITE");
            System.out.println(dumpPlan);
        }

        return impl.visitChild(this, 0, inputAsEnum, pref);
    }

    private Result buildHiveResult(EnumerableRelImplementor enumImplementor, Prefer pref, OLAPContext context) {
        RelDataType hiveRowType = getRowType();

        context.olapRowType = hiveRowType;
        PhysType physType = PhysTypeImpl.of(enumImplementor.getTypeFactory(), hiveRowType, pref.preferArray());

        RelOptTable factTable = context.firstTableScan.getTable();
        Result result = enumImplementor.result(physType, Blocks.toBlock(Expressions.call(factTable.getExpression(OLAPTable.class), "executeHiveQuery", enumImplementor.getRootExpression())));
        return result;
    }

}

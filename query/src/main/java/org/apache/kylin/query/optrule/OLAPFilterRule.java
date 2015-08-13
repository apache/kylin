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

package org.apache.kylin.query.optrule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.kylin.query.relnode.OLAPFilterRel;
import org.apache.kylin.query.relnode.OLAPRel;

/**
 */
public class OLAPFilterRule extends RelOptRule {

    public static final RelOptRule INSTANCE = new OLAPFilterRule();

    public OLAPFilterRule() {
        super(operand(LogicalFilter.class, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);

        RelTraitSet origTraitSet = filter.getTraitSet();
        RelTraitSet traitSet = origTraitSet.replace(OLAPRel.CONVENTION).simplify();

        OLAPFilterRel olapFilter = new OLAPFilterRel(filter.getCluster(), traitSet, convert(filter.getInput(), traitSet), filter.getCondition());
        call.transformTo(olapFilter);
    }

}

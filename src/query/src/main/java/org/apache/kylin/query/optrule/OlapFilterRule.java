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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.kylin.query.relnode.OlapFilterRel;
import org.apache.kylin.query.relnode.OlapRel;

public class OlapFilterRule extends ConverterRule {

    public static final RelOptRule INSTANCE = new OlapFilterRule();

    public OlapFilterRule() {
        super(LogicalFilter.class, RelOptUtil.FILTER_PREDICATE, Convention.NONE, OlapRel.CONVENTION, "OlapFilterRule");
    }

    @Override
    public RelNode convert(RelNode call) {
        LogicalFilter filter = (LogicalFilter) call;
        RelTraitSet origTraitSet = filter.getTraitSet();
        RelTraitSet traitSet = origTraitSet.replace(OlapRel.CONVENTION).simplify();

        RelNode convertedInput = filter.getInput() instanceof HepRelVertex ? filter.getInput()
                : convert(filter.getInput(), OlapRel.CONVENTION);
        return new OlapFilterRel(filter.getCluster(), traitSet, convertedInput, filter.getCondition());
    }

}

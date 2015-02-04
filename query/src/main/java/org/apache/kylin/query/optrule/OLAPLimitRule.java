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

import org.apache.kylin.query.relnode.OLAPLimitRel;
import org.apache.kylin.query.relnode.OLAPRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;

/**
 * 
 * @author xjiang
 * 
 */
public class OLAPLimitRule extends RelOptRule {

    public static final RelOptRule INSTANCE = new OLAPLimitRule();

    public OLAPLimitRule() {
        super(operand(SortRel.class, any()), "OLAPLimitRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final SortRel sort = call.rel(0);
        if (sort.offset == null && sort.fetch == null) {
            return;
        }
        final RelTraitSet traitSet = sort.getTraitSet().replace(OLAPRel.CONVENTION);
        RelNode input = sort.getChild();
        if (!sort.getCollation().getFieldCollations().isEmpty()) {
            // Create a sort with the same sort key, but no offset or fetch.
            input = sort.copy(sort.getTraitSet(), input, sort.getCollation(), null, null);
        }
        RelNode x = convert(input, input.getTraitSet().replace(OLAPRel.CONVENTION));
        call.transformTo(new OLAPLimitRel(sort.getCluster(), traitSet, x, sort.offset, sort.fetch));
    }

}

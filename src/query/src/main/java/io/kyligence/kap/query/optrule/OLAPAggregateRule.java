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

package io.kyligence.kap.query.optrule;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.kylin.query.relnode.OLAPAggregateRel;
import org.apache.kylin.query.relnode.OLAPRel;

/**
 */
public class OLAPAggregateRule extends ConverterRule {

    public static final ConverterRule INSTANCE = new OLAPAggregateRule();

    public OLAPAggregateRule() {
        super(LogicalAggregate.class, Convention.NONE, OLAPRel.CONVENTION, "OLAPAggregateRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalAggregate agg = (LogicalAggregate) rel;

        // AVG() will be transformed into SUM()/COUNT() by AggregateReduceFunctionsRule.
        // Here only let the transformed plan pass.
        if (containsAvg(agg)) {
            return null;
        }

        RelTraitSet traitSet = agg.getTraitSet().replace(OLAPRel.CONVENTION);
        try {
            return new OLAPAggregateRel(agg.getCluster(), traitSet, convert(agg.getInput(), OLAPRel.CONVENTION),
                    agg.indicator, agg.getGroupSet(), agg.getGroupSets(), agg.getAggCallList());
        } catch (InvalidRelException e) {
            throw new IllegalStateException("Can't create OLAPAggregateRel!", e);
        }
    }

    private boolean containsAvg(LogicalAggregate agg) {
        for (AggregateCall call : agg.getAggCallList()) {
            SqlAggFunction func = call.getAggregation();
            if (func instanceof SqlAvgAggFunction)
                return true;
        }
        return false;
    }

}

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

import org.apache.kylin.query.relnode.OLAPFilterRel;
import org.apache.kylin.query.relnode.OLAPJoinRel;
import org.apache.kylin.query.relnode.OLAPRel;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinInfo;
import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;

/**
 * 
 * @author xjiang
 * 
 */
public class OLAPJoinRule extends ConverterRule {

    public static final ConverterRule INSTANCE = new OLAPJoinRule();

    public OLAPJoinRule() {
        super(JoinRel.class, Convention.NONE, OLAPRel.CONVENTION, "OLAPJoinRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        JoinRel join = (JoinRel) rel;
        RelNode left = join.getInput(0);
        RelNode right = join.getInput(1);

        RelTraitSet traitSet = join.getTraitSet().replace(OLAPRel.CONVENTION);
        left = convert(left, traitSet);
        right = convert(right, traitSet);

        final JoinInfo info = JoinInfo.of(left, right, join.getCondition());
        if (!info.isEqui() && join.getJoinType() != JoinRelType.INNER) {
            // EnumerableJoinRel only supports equi-join. We can put a filter on top
            // if it is an inner join.
            return null;
        }

        RelOptCluster cluster = join.getCluster();
        RelNode newRel;
        try {
            newRel = new OLAPJoinRel(cluster, traitSet, left, right, //
                    info.getEquiCondition(left, right, cluster.getRexBuilder()), //
                    info.leftKeys, info.rightKeys, join.getJoinType(), join.getVariablesStopped());
        } catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to
            // internal error.
            throw new AssertionError(e);
            // LOGGER.fine(e.toString());
            // return null;
        }
        if (!info.isEqui()) {
            newRel = new OLAPFilterRel(cluster, newRel.getTraitSet(), newRel, info.getRemaining(cluster.getRexBuilder()));
        }
        return newRel;
    }

}

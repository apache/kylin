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
package com.kylinolap.query.optrule;

import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelTraitSet;

import com.kylinolap.query.relnode.OLAPJoinRel;
import com.kylinolap.query.relnode.OLAPRel;

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
        JoinRel joinRel = (JoinRel) rel;
        RelNode leftRel = joinRel.getInput(0);
        RelNode rightRel = joinRel.getInput(1);
        RelTraitSet traitSet = joinRel.getTraitSet().replace(OLAPRel.CONVENTION);
        try {
            return new OLAPJoinRel(joinRel.getCluster(), traitSet, convert(leftRel, traitSet), convert(rightRel, traitSet), joinRel.getCondition(), joinRel.getJoinType(), joinRel.getVariablesStopped());
        } catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to
            // internal error.
            throw new AssertionError(e);
        }
    }

}

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

import org.apache.kylin.query.relnode.OLAPRel;
import org.eigenbase.rel.AggregateRel;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelTraitSet;

import org.apache.kylin.query.relnode.OLAPAggregateRel;

/**
 * 
 * @author xjiang
 * 
 */
public class OLAPAggregateRule extends ConverterRule {

    public static final ConverterRule INSTANCE = new OLAPAggregateRule();

    public OLAPAggregateRule() {
        super(AggregateRel.class, Convention.NONE, OLAPRel.CONVENTION, "OLAPAggregateRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        AggregateRel agg = (AggregateRel) rel;
        RelTraitSet traitSet = agg.getTraitSet().replace(OLAPRel.CONVENTION);
        try {
            return new OLAPAggregateRel(agg.getCluster(), traitSet, convert(agg.getChild(), traitSet), agg.getGroupSet(), agg.getAggCallList());
        } catch (InvalidRelException e) {
            throw new IllegalStateException("Can't create OLAPAggregateRel!", e);
        }
    }

}

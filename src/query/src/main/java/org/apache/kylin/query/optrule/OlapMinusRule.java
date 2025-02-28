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

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Minus;
import org.apache.kylin.query.relnode.OlapMinusRel;
import org.apache.kylin.query.relnode.OlapRel;

public class OlapMinusRule extends ConverterRule {

    public static final OlapMinusRule INSTANCE = new OlapMinusRule();

    public OlapMinusRule() {
        super(Minus.class, Convention.NONE, OlapRel.CONVENTION, "OlapMinusRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        final Minus minus = (Minus) rel;
        final RelTraitSet traitSet = minus.getTraitSet().replace(OlapRel.CONVENTION);
        final List<RelNode> inputs = minus.getInputs();
        return new OlapMinusRel(rel.getCluster(), traitSet, convertList(inputs, OlapRel.CONVENTION), minus.all);
    }
}

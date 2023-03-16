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

package org.apache.kylin.query.util;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.KapRel;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPRel;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FirstRoundContextCutStrategy implements ICutContextStrategy {

    @Override
    public List<OLAPRel> cutOffContext(OLAPRel rootRel, RelNode parentOfRoot) {
        //Step 1.first round, cutting olap context
        KapRel.OLAPContextImplementor contextImplementor = new KapRel.OLAPContextImplementor();
        KapRel.ContextVisitorState initState = KapRel.ContextVisitorState.init();
        contextImplementor.visitChild(rootRel, rootRel, initState);
        if (initState.hasFreeTable()) {
            // if there are free tables, allocate a context for it
            contextImplementor.allocateContext((KapRel) rootRel, parentOfRoot);
        }
        toLeafJoinForm();

        ContextUtil.dumpCalcitePlan("EXECUTION PLAN AFTER HEP PLANNER", rootRel, log);
        contextImplementor.optimizeContextCut();
        return Lists.newArrayList(rootRel);
    }

    @Override
    public boolean needCutOff(OLAPRel rootRel) {
        return true;
    }

    private void toLeafJoinForm() {
        // filter and project pull up
        for (OLAPContext context : ContextUtil.listContexts()) {
            RelNode parentOfTopNode = context.getParentOfTopNode();
            if (parentOfTopNode == null) {
                for (int i = 0; i < context.getTopNode().getInputs().size(); i++) {
                    context.getTopNode().replaceInput(i,
                            HepUtils.runRuleCollection(context.getTopNode().getInput(i), HepUtils.CUBOID_OPT_RULES));
                }
                ((KapRel) context.getTopNode()).setContext(context);
                continue;
            }

            for (int i = 0; i < parentOfTopNode.getInputs().size(); i++) {
                if (context.getTopNode() != parentOfTopNode.getInput(i))
                    continue;

                RelNode newInput = HepUtils.runRuleCollection(parentOfTopNode.getInput(i), HepUtils.CUBOID_OPT_RULES);
                ((KapRel) newInput).setContext(context);
                context.setTopNode((KapRel) newInput);
                parentOfTopNode.replaceInput(i, newInput);
            }
        }
    }
}

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
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.relnode.OlapRel;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ContextInitialCutStrategy implements ICutContextStrategy {

    @Override
    public List<OlapRel> cutOffContext(OlapRel rootRel, RelNode parentOfRoot) {
        //Step 1.first round, cutting olap context
        OlapRel.ContextImpl contextImpl = new OlapRel.ContextImpl();
        OlapRel.ContextVisitorState initState = OlapRel.ContextVisitorState.init();
        contextImpl.visitChild(rootRel, rootRel, initState);
        if (initState.hasFreeTable()) {
            // if there are free tables, allocate a context for it
            contextImpl.allocateContext(rootRel, parentOfRoot);
        }
        toLeafJoinForm();

        ContextUtil.dumpCalcitePlan("EXECUTION PLAN AFTER HEP PLANNER", rootRel, log);
        contextImpl.optimizeContextCut();
        return Lists.newArrayList(rootRel);
    }

    @Override
    public boolean needCutOff(OlapRel rootRel) {
        return true;
    }

    private void toLeafJoinForm() {
        // filter and project pull up
        for (OlapContext context : ContextUtil.listContexts()) {
            RelNode parentOfTopNode = context.getParentOfTopNode();
            if (parentOfTopNode == null) {
                for (int i = 0; i < context.getTopNode().getInputs().size(); i++) {
                    context.getTopNode().replaceInput(i,
                            HepUtils.runRuleCollection(context.getTopNode().getInput(i), HepUtils.CUBOID_OPT_RULES));
                }
                context.getTopNode().setContext(context);
                continue;
            }

            for (int i = 0; i < parentOfTopNode.getInputs().size(); i++) {
                if (context.getTopNode() != parentOfTopNode.getInput(i))
                    continue;

                RelNode newInput = HepUtils.runRuleCollection(parentOfTopNode.getInput(i), HepUtils.CUBOID_OPT_RULES);
                ((OlapRel) newInput).setContext(context);
                context.setTopNode((OlapRel) newInput);
                parentOfTopNode.replaceInput(i, newInput);
            }
        }
    }
}

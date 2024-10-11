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
import org.apache.kylin.metadata.cube.cuboid.NLookupCandidate;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.relnode.OlapRel;
import org.apache.kylin.query.relnode.OlapTableScan;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ContextReCutStrategy implements ICutContextStrategy {

    private ContextCutImpl reCutter;

    @Override
    public List<OlapRel> cutOffContext(OlapRel rootRel, RelNode parentOfRoot) {
        for (OlapTableScan tableScan : rootRel.getContext().getAllTableScans()) {
            tableScan.setColumnRowType(null);
        }
        // pre-order travel tree, re-cut context to smaller contexts
        OlapContext originCtx = rootRel.getContext();
        reCutter.visitChild(rootRel);
        ContextUtil.clearThreadLocalContextById(originCtx.getId());
        return Lists.newArrayList(rootRel);
    }

    @Override
    public boolean needCutOff(OlapRel rootRel) {
        return rootRel.getContext() != null && rootRel.getContext().isHasJoin();
    }

    void tryCutToSmallerContexts(RelNode root, RuntimeException e) {
        ContextCutImpl cutter = getReCutter() == null //
                ? new ContextCutImpl(ContextUtil.getThreadLocalContexts().size())
                : new ContextCutImpl(getReCutter().getCtxSeq());
        setReCutter(cutter);
        ContextUtil.listContextsHavingScan().stream()
                .filter(context -> context.deduceLookupTableType() == NLookupCandidate.Policy.NONE).forEach(context -> {
                    if (context.isHasSelected() && context.getRealization() == null
                            && (!context.isHasPreCalcJoin() || context.getBoundedModelAlias() != null)) {
                        throw e;
                    } else if (context.isHasSelected() && context.getRealization() == null) {
                        QueryContextCutter.cutContext(this, context.getTopNode(), root);
                        ContextUtil.setSubContexts(root.getInput(0));
                        return;
                    } else if (context.getRealization() != null) {
                        context.unfixModel();
                    }
                    context.clearCtxInfo();
                });
    }
}

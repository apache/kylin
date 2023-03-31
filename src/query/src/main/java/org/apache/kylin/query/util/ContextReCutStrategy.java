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
import java.util.Objects;

import org.apache.calcite.rel.RelNode;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPRel;
import org.apache.kylin.query.relnode.OLAPTableScan;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ContextReCutStrategy implements ICutContextStrategy {

    private CutContextImplementor reCutter;

    @Override
    public List<OLAPRel> cutOffContext(OLAPRel rootRel, RelNode parentOfRoot) {
        for (OLAPTableScan tableScan : rootRel.getContext().allTableScans) {
            tableScan.setColumnRowType(null);
        }
        // pre-order travel tree, recut context to smaller contexts
        OLAPContext originCtx = rootRel.getContext();
        reCutter.visitChild(rootRel);
        OLAPContext.clearThreadLocalContextById(originCtx.id);
        return Lists.newArrayList(rootRel);
    }

    @Override
    public boolean needCutOff(OLAPRel rootRel) {
        return rootRel.getContext() != null && rootRel.getContext().isHasJoin();
    }

    void tryCutToSmallerContexts(RelNode root, RuntimeException e) {
        ICutContextStrategy.CutContextImplementor cutter = getReCutter() == null
                ? new ICutContextStrategy.CutContextImplementor(
                        Objects.requireNonNull(OLAPContext.getThreadLocalContexts()).size())
                : new ICutContextStrategy.CutContextImplementor(getReCutter().getCtxSeq());
        setReCutter(cutter);
        for (OLAPContext context : ContextUtil.listContextsHavingScan()) {
            if (context.isHasSelected() && context.realization == null
                    && (!context.isHasPreCalcJoin() || context.getModelAlias() != null)) {
                throw e;
            } else if (context.isHasSelected() && context.realization == null) {
                QueryContextCutter.cutContext(this, context.getTopNode(), root);
                ContextUtil.setSubContexts(root.getInput(0));
                continue;
            } else if (context.realization != null) {
                context.unfixModel();
            }
            context.clearCtxInfo();
        }
    }
}

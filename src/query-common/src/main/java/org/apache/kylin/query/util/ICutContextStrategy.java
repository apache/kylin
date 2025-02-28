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
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.relnode.OlapRel;

public interface ICutContextStrategy {

    List<OlapRel> cutOffContext(OlapRel rootRel, RelNode parentOfRoot);

    boolean needCutOff(OlapRel rootRel);

    class ContextCutImpl {
        private int ctxSeq;

        public ContextCutImpl(int ctxSeq) {
            this.ctxSeq = ctxSeq;
        }

        public void visitChild(RelNode input) {
            ((OlapRel) input).implementCutContext(this);
        }

        public OlapContext allocateContext(OlapRel topNode, RelNode parentOfTopNode) {
            OlapContext context = new OlapContext(ctxSeq++);
            ContextUtil.registerContext(context);
            context.setTopNode(topNode);
            topNode.setContext(context);
            context.setParentOfTopNode(parentOfTopNode);
            return context;
        }

        public int getCtxSeq() {
            return ctxSeq;
        }
    }
}

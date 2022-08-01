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
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPRel;
import org.apache.kylin.query.relnode.KapRel;

public interface ICutContextStrategy {

    List<OLAPRel> cutOffContext(OLAPRel rootRel, RelNode parentOfRoot);

    boolean needCutOff(OLAPRel rootRel);

    class CutContextImplementor {
        private int ctxSeq;

        public CutContextImplementor(int ctxSeq) {
            this.ctxSeq = ctxSeq;
        }

        public void visitChild(RelNode input) {
            ((KapRel) input).implementCutContext(this);
        }

        public OLAPContext allocateContext(KapRel topNode, RelNode parentOfTopNode) {
            OLAPContext context = new OLAPContext(ctxSeq++);
            OLAPContext.registerContext(context);
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

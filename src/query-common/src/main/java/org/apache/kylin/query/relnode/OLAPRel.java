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

package org.apache.kylin.query.relnode;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Stack;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

/**
 */
public interface OLAPRel extends RelNode {

    public static final Logger logger = LoggerFactory.getLogger(OLAPRel.class);

    // Calling convention for relational operations that occur in OLAP.
    public static final Convention CONVENTION = new Convention.Impl("OLAP", OLAPRel.class);

    /**
     * get olap context
     */
    public OLAPContext getContext();

    /**
     * get the row type of ColumnDesc
     *
     * @return
     */
    public ColumnRowType getColumnRowType();

    /**
     * whether has sub query
     */
    public boolean hasSubQuery();

    /**
     * replace RelTraitSet
     */
    public RelTraitSet replaceTraitSet(RelTrait trait);

    public void implementOLAP(OLAPImplementor implementor);

    public void implementRewrite(RewriteImplementor rewriter);

    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs);

    /**
     * visitor pattern for olap query analysis
     */
    public static class OLAPImplementor {

        private Stack<RelNode> parentNodeStack = new Stack<>();
        private int ctxSeq = 0;
        private Stack<OLAPContext> ctxStack = new Stack<>();
        private boolean newOLAPContextRequired = false;

        public void visitChild(RelNode input, RelNode parentNode) {
            this.parentNodeStack.push(parentNode);
            ((OLAPRel) input).implementOLAP(this);
            this.parentNodeStack.pop();
        }

        public RelNode getParentNode() {
            return parentNodeStack.peek();
        }

        public Stack<RelNode> getParentNodeStack() {
            return parentNodeStack;
        }

        public OLAPContext getContext() {
            if (ctxStack.isEmpty()) {
                return null;
            }
            return ctxStack.peek();
        }

        public void freeContext() {
            ctxStack.pop();
        }

        public void allocateContext() {
            OLAPContext context = new OLAPContext(ctxSeq++);
            ctxStack.push(context);
            OLAPContext.registerContext(context);
            setNewOLAPContextRequired(false);
        }

        public boolean isNewOLAPContextRequired() {
            return this.newOLAPContextRequired;
        }

        // set the flag to let OLAPImplementor allocate a new context more proactively
        public void setNewOLAPContextRequired(boolean newOLAPContextRequired) {
            this.newOLAPContextRequired = newOLAPContextRequired;
        }

        public void fixSharedOlapTableScan(SingleRel parent) {
            OLAPTableScan copy = copyTableScanIfNeeded(parent.getInput());
            if (copy != null)
                parent.replaceInput(0, copy);
        }

        public void fixSharedOlapTableScanOnTheLeft(BiRel parent) {
            OLAPTableScan copy = copyTableScanIfNeeded(parent.getLeft());
            if (copy != null)
                parent.replaceInput(0, copy);
        }

        public void fixSharedOlapTableScanOnTheRight(BiRel parent) {
            OLAPTableScan copy = copyTableScanIfNeeded(parent.getRight());
            if (copy != null)
                parent.replaceInput(1, copy);
        }

        public void fixSharedOlapTableScanAt(RelNode parent, int ordinalInParent) {
            OLAPTableScan copy = copyTableScanIfNeeded(parent.getInputs().get(ordinalInParent));
            if (copy != null)
                parent.replaceInput(ordinalInParent, copy);
        }

        private OLAPTableScan copyTableScanIfNeeded(RelNode input) {
            if (input instanceof OLAPTableScan) {
                OLAPTableScan tableScan = (OLAPTableScan) input;
                if (tableScan.getColumnRowType() != null) { // implementedOLAP() was done, meaning it is shared
                    return (OLAPTableScan) tableScan.copy(tableScan.getTraitSet(), tableScan.getInputs());
                }
            }
            return null;
        }
    }

    /**
     * visitor pattern for query rewrite
     */

    public static class RewriteImplementor {
        private OLAPContext parentContext;

        public static boolean needRewrite(OLAPContext ctx) {
            if (ctx.isHasJoin())
                return true;

            if (ctx.realization == null) {
                return false;
            }

            String realRootFact = ctx.realization.getModel().getRootFactTable().getTableIdentity();
            return ctx.firstTableScan.getTableName().equals(realRootFact);
        }

        public void visitChild(RelNode parent, RelNode child) {
            if (parent instanceof OLAPRel) {
                OLAPRel olapRel = (OLAPRel) parent;
                this.parentContext = olapRel.getContext();
            }
            OLAPRel olapChild = (OLAPRel) child;
            olapChild.implementRewrite(this);
        }

        public OLAPContext getParentContext() {
            return parentContext;
        }
    }

    /**
     * implementor for java generation
     */
    @Slf4j
    public static class JavaImplementor extends EnumerableRelImplementor {

        private IdentityHashMap<EnumerableRel, OLAPContext> relContexts = Maps.newIdentityHashMap();
        private boolean calciteDebug = System.getProperty("calcite.debug") != null;

        public JavaImplementor(EnumerableRelImplementor enumImplementor) {
            super(enumImplementor.getRexBuilder(), new LinkedHashMap<>());
        }

        public EnumerableRel createEnumerable(OLAPRel parent) {
            ArrayList<EnumerableRel> enumInputs = null;
            List<RelNode> children = parent.getInputs();
            if (children != null) {
                enumInputs = Lists.newArrayListWithCapacity(children.size());
                for (RelNode child : children) {
                    enumInputs.add(createEnumerable((OLAPRel) child));
                }
            }

            EnumerableRel result = parent.implementEnumerable(enumInputs);
            relContexts.put(result, parent.getContext());
            return result;
        }

        @Override
        public EnumerableRel.Result visitChild(EnumerableRel parent, int ordinal, EnumerableRel child,
                EnumerableRel.Prefer prefer) {
            if (calciteDebug) {
                OLAPContext context;
                if (child instanceof OLAPRel)
                    context = ((OLAPRel) child).getContext();
                else
                    context = relContexts.get(child);
                log.info(context + " - " + child);
            }

            return super.visitChild(parent, ordinal, child, prefer);
        }
    }

}

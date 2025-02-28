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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.query.util.ICutContextStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 */
public interface OlapRel extends RelNode {

    Logger logger = LoggerFactory.getLogger(OlapRel.class);

    // Calling convention for relational operations that occur in OLAP.
    Convention CONVENTION = new Convention.Impl("OLAP", OlapRel.class);
    // olapRel default cost factor
    double OLAP_COST_FACTOR = 0.05;

    /** get olap context */
    OlapContext getContext();

    /** Get column row type. */
    ColumnRowType getColumnRowType();

    /** Returns true if this relNode has subQuery or its children have subQuery. */
    boolean hasSubQuery();

    /** replace RelTraitSet */
    RelTraitSet replaceTraitSet(RelTrait trait);

    void implementOlap(OlapImpl implementor);

    void implementRewrite(RewriteImpl rewriter);

    EnumerableRel implementEnumerable(List<EnumerableRel> inputs);

    /**
     * To allocate context for the nodes before OLAP implement.
     * Some nodes don't have a context.
     * @param contextImpl the visitor
     * @param state the state returned after visit
     */
    void implementContext(ContextImpl contextImpl, ContextVisitorState state);

    /**
     * To cut off context if context is too big and no realization can serve it
     *
     * @param implementor implementation of contextCutter.
     */
    void implementCutContext(ICutContextStrategy.ContextCutImpl implementor);

    /**
     * Set context to this node and all children nodes, even the undirected children.
     * @param context The context to be set.
     */
    void setContext(OlapContext context);

    /**
     * Absorbs the current node into the given OlapContext.
     * @param context The give OlapContext.
     */
    boolean pushRelInfoToContext(OlapContext context);

    Set<OlapContext> getSubContexts();

    void setSubContexts(Set<OlapContext> contexts);

    default String displayCtxId(OlapContext ctx) {
        return ctx == null ? "" : ctx.getId() + "@" + ctx.getRealization();
    }

    /**
     * visitor pattern for olap query analysis
     */
    class OlapImpl {

        private final Deque<RelNode> parentNodeStack = new ArrayDeque<>();

        public void visitChild(RelNode input, RelNode parentNode) {
            this.parentNodeStack.offerLast(parentNode);
            ((OlapRel) input).implementOlap(this);
            this.parentNodeStack.pollLast();
        }

        public Deque<RelNode> getParentNodeStack() {
            return parentNodeStack;
        }

        public RelNode getParentNode() {
            return parentNodeStack.peekLast();
        }
    }

    /**
     * visitor pattern for query rewrite
     */

    class RewriteImpl {
        @Getter
        private OlapContext parentContext;

        public static boolean needRewrite(OlapContext ctx) {
            if (ctx.isHasJoin()) {
                return true;
            }

            if (ctx.getRealization() == null) {
                return false;
            }

            String realRootFact = ctx.getRealization().getModel().getRootFactTable().getTableIdentity();
            return ctx.getFirstTableScan().getTableName().equals(realRootFact);
        }

        public void visitChild(RelNode parent, RelNode child) {
            if (parent instanceof OlapRel) {
                OlapRel olapRel = (OlapRel) parent;
                this.parentContext = olapRel.getContext();
            }
            OlapRel olapChild = (OlapRel) child;
            olapChild.implementRewrite(this);
        }
    }

    /**
     * implementor for java generation
     */
    @Slf4j
    class JavaImplementor extends EnumerableRelImplementor {

        private final IdentityHashMap<EnumerableRel, OlapContext> relContexts = Maps.newIdentityHashMap();
        private final boolean calciteDebug = System.getProperty("calcite.debug") != null;

        public JavaImplementor(EnumerableRelImplementor enumImplementor) {
            super(enumImplementor.getRexBuilder(), new LinkedHashMap<>());
        }

        public EnumerableRel createEnumerable(OlapRel parent) {
            ArrayList<EnumerableRel> enumInputs = null;
            List<RelNode> children = parent.getInputs();
            if (children != null) {
                enumInputs = Lists.newArrayListWithCapacity(children.size());
                for (RelNode child : children) {
                    enumInputs.add(createEnumerable((OlapRel) child));
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
                OlapContext context;
                if (child instanceof OlapRel)
                    context = ((OlapRel) child).getContext();
                else
                    context = relContexts.get(child);
                log.info(context + " - " + child);
            }

            return super.visitChild(parent, ordinal, child, prefer);
        }
    }

    /**
     * visitor pattern for cutting OLAP query contexts
     */
    class ContextImpl {

        @Setter
        @Getter
        private TableDesc firstTableDesc;

        private final Deque<RelNode> parentNodeStack = new ArrayDeque<>();
        private int ctxSeq = 0;
        private final Queue<RelNode> aggRelQueue = new LinkedList<>();

        /**
         * @param input      child rel node
         * @param parentNode parent rel node
         * @param state      it's actually return value
         */
        public void visitChild(RelNode input, RelNode parentNode, ContextVisitorState state) {
            this.parentNodeStack.offerLast(parentNode);
            ((OlapRel) input).implementContext(this, state);
            if (input instanceof OlapAggregateRel)
                addAgg(input);
            this.parentNodeStack.pollLast();
        }

        public OlapContext allocateContext(OlapRel topNode, RelNode parentOfTopNode) {
            OlapContext context = new OlapContext(ctxSeq++);
            ContextUtil.registerContext(context);
            context.setTopNode(topNode);
            context.setParentOfTopNode(parentOfTopNode);
            topNode.setContext(context);
            return context;
        }

        public void fixSharedOlapTableScan(SingleRel parent) {
            OlapTableScan copy = copyTableScanIfNeeded(parent.getInput());
            if (copy != null)
                parent.replaceInput(0, copy);
        }

        public void fixSharedOlapTableScanOnTheLeft(BiRel parent) {
            OlapTableScan copy = copyTableScanIfNeeded(parent.getLeft());
            if (copy != null)
                parent.replaceInput(0, copy);
        }

        public void fixSharedOlapTableScanOnTheRight(BiRel parent) {
            OlapTableScan copy = copyTableScanIfNeeded(parent.getRight());
            if (copy != null)
                parent.replaceInput(1, copy);
        }

        public void fixSharedOlapTableScanAt(RelNode parent, int ordinalInParent) {
            OlapTableScan copy = copyTableScanIfNeeded(parent.getInputs().get(ordinalInParent));
            if (copy != null)
                parent.replaceInput(ordinalInParent, copy);
        }

        private OlapTableScan copyTableScanIfNeeded(RelNode input) {
            if (input instanceof OlapTableScan) {
                OlapTableScan tableScan = (OlapTableScan) input;
                if (tableScan.isContextVisited()) { // this node has been visited before, should copy it
                    return (OlapTableScan) tableScan.copy(tableScan.getTraitSet(), tableScan.getInputs());
                }
            }
            return null;
        }

        // collect every Agg rel to optimize the logic execution plan
        public void addAgg(RelNode relNode) {
            this.aggRelQueue.add(relNode);
        }

        public void optimizeContextCut() {
            RelNode rel = this.aggRelQueue.poll();
            while (rel != null) {
                ((OlapAggregateRel) rel).optimizeContextCut();
                rel = this.aggRelQueue.poll();
            }
        }
    }

    @Setter
    class ContextVisitorState {

        private boolean hasFilter; // filter exists in the child
        private boolean hasFreeTable; // free table (not in any context) exists in the child
        private boolean hasIncrementalTable;
        private boolean hasFirstTable;
        private boolean hasModelView;

        public ContextVisitorState(boolean hasFilter, boolean hasFreeTable, boolean hasIncrementalTable) {
            this.hasFilter = hasFilter;
            this.hasFreeTable = hasFreeTable;
            this.hasIncrementalTable = hasIncrementalTable;
        }

        // Use cache to improve performance?
        public static ContextVisitorState of(boolean hasFilter, boolean hasFreeTable) {
            return of(hasFilter, hasFreeTable, false);
        }

        public static ContextVisitorState of(boolean hasFilter, boolean hasFreeTable, boolean hasIncrementalTable) {
            return new ContextVisitorState(hasFilter, hasFreeTable, hasIncrementalTable);
        }

        public static ContextVisitorState init() {
            return of(false, false, false);
        }

        public boolean hasFirstTable() {
            return hasFirstTable;
        }

        public boolean hasIncrementalTable() {
            return this.hasIncrementalTable;
        }

        public boolean hasFilter() {
            return this.hasFilter;
        }

        public boolean hasFreeTable() {
            return this.hasFreeTable;
        }

        public boolean hasModelView() {
            return this.hasModelView;
        }

        public ContextVisitorState merge(ContextVisitorState that) {
            this.hasFilter = that.hasFilter || this.hasFilter;
            this.hasFreeTable = that.hasFreeTable || this.hasFreeTable;
            this.hasIncrementalTable = that.hasIncrementalTable || this.hasIncrementalTable;
            this.hasFirstTable = that.hasFirstTable || this.hasFirstTable;
            this.hasModelView = that.hasModelView || this.hasModelView;

            return this;
        }
    }
}

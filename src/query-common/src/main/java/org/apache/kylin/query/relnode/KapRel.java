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

import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.query.util.ICutContextStrategy;

import lombok.Getter;
import lombok.Setter;

public interface KapRel extends OLAPRel {
    /**
     * To allocate context for the nodes before OLAP implement.
     * Some nodes don't have a context.
     * @param olapContextImplementor the visitor
     * @param state the state returned after visit
     */
    void implementContext(OLAPContextImplementor olapContextImplementor, ContextVisitorState state);

    /**
     * To cut off context if context is too big and no realization can serve it
     *
     * @param implementor
     */
    void implementCutContext(ICutContextStrategy.CutContextImplementor implementor);

    /**
     * Set context to this node and all children nodes, even the undirected children.
     * @param context The context to be set.
     */
    void setContext(OLAPContext context);

    /**
     * add rel to some context
     * case1 :
     *      when AggRel above INNER JOIN rel not belong to any Context and
     *      its all agg derived from the same one of subContext.
     * @param context
     */
    boolean pushRelInfoToContext(OLAPContext context);

    Set<OLAPContext> getSubContext();

    void setSubContexts(Set<OLAPContext> contexts);

    /**
     * visitor pattern for cutting OLAP query contexts
     */
    class OLAPContextImplementor {

        @Setter
        @Getter
        private TableDesc firstTableDesc;

        private Stack<RelNode> parentNodeStack = new Stack<>();
        private int ctxSeq = 0;
        private Queue<RelNode> aggRelQueue = new LinkedList<>();

        /**
         * @param input      child rel node
         * @param parentNode parent rel node
         * @param state      it's actually return value
         */
        public void visitChild(RelNode input, RelNode parentNode, ContextVisitorState state) {
            this.parentNodeStack.push(parentNode);
            ((KapRel) input).implementContext(this, state);
            if (input instanceof KapAggregateRel)
                addAgg(input);
            this.parentNodeStack.pop();
        }

        public RelNode getParentNode() {
            return parentNodeStack.peek();
        }

        public OLAPContext allocateContext(KapRel topNode, RelNode parentOfTopNode) {
            OLAPContext context = new OLAPContext(ctxSeq++);
            OLAPContext.registerContext(context);
            context.setTopNode(topNode);
            context.setParentOfTopNode(parentOfTopNode);
            topNode.setContext(context);
            return context;
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

        public Stack<RelNode> getParentNodeStack() {
            return parentNodeStack;
        }

        private OLAPTableScan copyTableScanIfNeeded(RelNode input) {
            if (input instanceof KapTableScan) {
                KapTableScan tableScan = (KapTableScan) input;
                if (tableScan.contextVisited) { // this node has been visited before, should copy it
                    return (OLAPTableScan) tableScan.copy(tableScan.getTraitSet(), tableScan.getInputs());
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
                ((KapAggregateRel) rel).optimizeContextCut();
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

        // TODO: Maybe cache is required to improve performance
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

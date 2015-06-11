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

import java.util.LinkedHashMap;
import java.util.Stack;

import net.hydromatic.optiq.rules.java.EnumerableRel;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelTraitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author xjiang
 * 
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

    /**
     * visitor pattern for olap query analysis
     */
    public static class OLAPImplementor {

        private RelNode parentNode = null;
        private int ctxSeq = 0;
        private Stack<OLAPContext> ctxStack = new Stack<OLAPContext>();

        public void visitChild(RelNode input, RelNode parentNode) {
            this.parentNode = parentNode;
            ((OLAPRel) input).implementOLAP(this);
        }

        public RelNode getParentNode() {
            return parentNode;
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
        }
    }

    public void implementOLAP(OLAPImplementor implementor);

    /**
     * visitor pattern for query rewrite
     */

    public static class RewriteImplementor {
        private OLAPContext parentContext;

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

        public static boolean needRewrite(OLAPContext ctx) {
            boolean hasFactTable = ctx.hasJoin || ctx.firstTableScan.getTableName().equals(ctx.realization.getFactTable());
            boolean hasRewriteFields = !ctx.rewriteFields.isEmpty();
            return hasRewriteFields && hasFactTable;
        }
    }

    public void implementRewrite(RewriteImplementor rewriter);

    /**
     * implementor for java generation
     */
    public static class JavaImplementor extends EnumerableRelImplementor {

        private OLAPContext parentContext;

        public JavaImplementor(EnumerableRelImplementor enumImplementor) {
            super(enumImplementor.getRexBuilder(), new LinkedHashMap<String, Object>());
        }

        public OLAPContext getParentContext() {
            return parentContext;
        }

        @Override
        public EnumerableRel.Result visitChild(EnumerableRel parent, int ordinal, EnumerableRel child, EnumerableRel.Prefer prefer) {
            if (parent instanceof OLAPRel) {
                OLAPRel olapRel = (OLAPRel) parent;
                this.parentContext = olapRel.getContext();
            }
            return super.visitChild(parent, ordinal, child, prefer);
        }
    }

}

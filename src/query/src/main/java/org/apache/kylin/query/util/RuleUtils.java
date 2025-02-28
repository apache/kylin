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

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Util;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.relnode.OlapJoinRel;

public class RuleUtils {

    private RuleUtils() {
    }

    public static boolean isJoinOnlyOneAggChild(OlapJoinRel joinRel) {
        RelNode joinLeftChild;
        RelNode joinRightChild;
        final RelNode joinLeft = joinRel.getLeft();
        final RelNode joinRight = joinRel.getRight();
        if (joinLeft instanceof RelSubset && joinRight instanceof RelSubset) {
            final RelSubset joinLeftChildSub = (RelSubset) joinLeft;
            final RelSubset joinRightChildSub = (RelSubset) joinRight;
            joinLeftChild = Util.first(joinLeftChildSub.getBest(), joinLeftChildSub.getOriginal());
            joinRightChild = Util.first(joinRightChildSub.getBest(), joinRightChildSub.getOriginal());

        } else if (joinLeft instanceof HepRelVertex && joinRight instanceof HepRelVertex) {
            joinLeftChild = ((HepRelVertex) joinLeft).getCurrentRel();
            joinRightChild = ((HepRelVertex) joinRight).getCurrentRel();
        } else {
            return false;
        }

        String project = QueryContext.current().getProject();
        if (project != null && NProjectManager.getProjectConfig(project).isEnhancedAggPushDownEnabled()
                && RelAggPushDownUtil.canRelAnsweredBySnapshot(project, joinRight)
                && RelAggPushDownUtil.isUnmatchedJoinRel(joinRel)) {
            QueryContext.current().setEnhancedAggPushDown(true);
            return true;
        }

        return isContainAggregate(joinLeftChild) ^ isContainAggregate(joinRightChild);
    }

    private static boolean isContainAggregate(RelNode node) {
        boolean[] isContainAggregate = new boolean[] { false };
        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (isContainAggregate[0]) {
                    // pruning
                    return;
                }
                RelNode relNode = node;
                if (node instanceof RelSubset) {
                    relNode = Util.first(((RelSubset) node).getBest(), ((RelSubset) node).getOriginal());
                } else if (node instanceof HepRelVertex) {
                    relNode = ((HepRelVertex) node).getCurrentRel();
                }
                if (relNode instanceof Aggregate) {
                    isContainAggregate[0] = true;
                }
                super.visit(relNode, ordinal, parent);
            }
        }.go(node);
        return isContainAggregate[0];
    }

    public static boolean isCast(RexNode rexNode) {
        if (!(rexNode instanceof RexCall)) {
            return false;
        }
        return SqlKind.CAST == rexNode.getKind();
    }

    public static boolean isPlainTableColumn(int colIdx, RelNode relNode) {
        if (relNode instanceof HepRelVertex) {
            relNode = ((HepRelVertex) relNode).getCurrentRel();
        }
        if (relNode instanceof TableScan) {
            return true;
        } else if (relNode instanceof Join) {
            Join join = (Join) relNode;
            int offset = 0;
            for (RelNode input : join.getInputs()) {
                if (colIdx >= offset && colIdx < offset + input.getRowType().getFieldCount()) {
                    return isPlainTableColumn(colIdx - offset, input);
                }
                offset += input.getRowType().getFieldCount();
            }
        } else if (relNode instanceof Project) {
            RexNode inputRex = ((Project) relNode).getProjects().get(colIdx);
            if (inputRex instanceof RexInputRef) {
                return isPlainTableColumn(((RexInputRef) inputRex).getIndex(), ((Project) relNode).getInput());
            }
        } else if (relNode instanceof Filter) {
            return isPlainTableColumn(colIdx, relNode.getInput(0));
        }
        return false;
    }

    public static boolean containCast(RexNode rexNode) {
        if (!(rexNode instanceof RexCall)) {
            return false;
        }
        if (SqlKind.CAST == rexNode.getKind()) {
            RexNode operand = ((RexCall) rexNode).getOperands().get(0);
            return !(operand instanceof RexCall) || operand.getKind() == SqlKind.CASE;
        }

        return false;
    }

    public static boolean isNotNullLiteral(RexNode node) {
        return !isNullLiteral(node);
    }

    public static boolean isNullLiteral(RexNode node) {
        return node instanceof RexLiteral && ((RexLiteral) node).isNull();
    }
}

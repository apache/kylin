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

package org.apache.kylin.rec.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.IQueryTransformer;
import org.apache.kylin.query.util.AbstractSqlVisitor;

import lombok.extern.slf4j.Slf4j;

/**
 * This Transformer will:
 * 1. ignore all non-first constants inner IN comma list within where condition.
 * BEFORE:
 *    select (a,1,2,'c',b,'d') from t where a in (1,2,3) and b in (a, 'd', 'f')
 * AFTER:
 *    select (a,1,2,'c',b,'d') from t where a in (1) and b in (a)
 *
 * 2. merge OR conditions
 * Before:
 *                       or                           or
 *                     /    \                       /    \
 *                   or    in_cond1               or     any
 *                  /  \                         /  \
 *           in_cond2  any                in_cond1  in_cond2
 * After:
 *                              or
 *                            /   \
 *             in_(cond1, cond2)   any
 */
@Slf4j
public class OptimizeTransformer implements IQueryTransformer {

    @Override
    public String transform(String originSql, String project, String defaultSchema) {
        try {
            KylinConfig config = NProjectManager.getProjectConfig(project);
            String sql = originSql;
            long start = System.currentTimeMillis();
            for (int i = 0; i < config.getOptimizeTransformerMaxIterations(); i++) {
                SqlNode node = CalciteParser.parse(sql);
                ConditionCounter counter = new ConditionCounter(originSql);
                MergeOrExprAndCollectInExprMatcher matcher = new MergeOrExprAndCollectInExprMatcher(originSql);
                if (counter.isBelowThreshold(node) || !matcher.isMergeableOrCollapsible(node)) {
                    break;
                }
                sql = updateCondition(sql, matcher.getToUpdateNodes());
            }
            log.debug("OptimizeTransformer cost: {} ms, the transformed sql is:\n{}",
                    System.currentTimeMillis() - start, sql);
            return sql;
        } catch (Exception e) {
            log.warn("Something unexpected in OptimizeTransformer, return original query", e);
            return originSql;
        }
    }

    private String updateCondition(String originSql, Map<SqlBasicCall, Action> toUpdateNodes) {
        List<SqlBasicCall> allNodes = new ArrayList<>(toUpdateNodes.keySet());
        Map<SqlBasicCall, Pair<Integer, Integer>> nodePositions = new HashMap<>();
        allNodes.forEach(node -> nodePositions.put(node, CalciteParser.getReplacePos(node, originSql)));
        allNodes.sort((o1, o2) -> nodePositions.get(o2).getFirst() - nodePositions.get(o1).getFirst());
        String sql = originSql + " "; // to avoid indexOutOfBound exception.
        for (SqlBasicCall call : allNodes) {
            Pair<Integer, Integer> pos = nodePositions.get(call);
            if (toUpdateNodes.get(call) == Action.DELETE) {
                sql = dropOrNode(sql, call, pos);
            } else if (toUpdateNodes.get(call) == Action.MODIFY) {
                sql = collapseNode(originSql, sql, call, pos);
            } else {
                throw new IllegalStateException("Not support action: " + toUpdateNodes.get(call));
            }
        }
        return sql.trim();
    }

    private String dropOrNode(String sql, SqlBasicCall call, Pair<Integer, Integer> pos) {
        String left = sql.substring(0, pos.getFirst()).trim();
        String right = sql.substring(pos.getSecond()).trim();
        while (left.endsWith("(") && right.startsWith(")")) {
            left = left.substring(0, left.length() - 1).trim();
            right = right.substring(1).trim();
        }
        if (left.length() > 2 && left.substring(left.length() - 2).equalsIgnoreCase("or")) {
            left = left.substring(0, left.length() - 2).trim();
        } else if (right.length() > 2 && right.substring(0, 2).equalsIgnoreCase("or")) {
            right = right.substring(2).trim();
        } else {
            throw new IllegalStateException("SqlNode could not be delete form sql: " + call);
        }
        return left + " " + right;
    }

    private String collapseNode(String originSql, String sql, SqlBasicCall in, Pair<Integer, Integer> pos) {
        assert in.getKind() == SqlKind.IN;
        assert in.operand(1) instanceof SqlNodeList;
        Set<String> newCommaList = new LinkedHashSet<>();
        for (SqlNode para : ((SqlNodeList) in.operand(1)).getList()) {
            if (newCommaList.isEmpty() || !(para instanceof SqlLiteral)) {
                newCommaList.add(nodeToString(para, originSql));
            }
        }
        String left = sql.substring(0, pos.getFirst()).trim();
        String right = sql.substring(pos.getSecond()).trim();
        if (newCommaList.size() == 1) {
            return left + " " + nodeToString(in.operand(0), originSql) + " = " + newCommaList.toArray()[0] + " "
                    + right;
        } else {
            return left + " " + nodeToString(in.operand(0), originSql) + " IN " + "(" + String.join(", ", newCommaList)
                    + ") " + right;
        }
    }

    private String nodeToString(SqlNode node, String originSql) {
        Pair<Integer, Integer> pos = CalciteParser.getReplacePos(node, originSql);
        return originSql.substring(pos.getFirst(), pos.getSecond());
    }

    private enum Action {
        MODIFY, DELETE
    }

    static class ConditionCounter extends AbstractSqlVisitor {
        int orConditionCnt = 0;

        protected ConditionCounter(String originSql) {
            super(originSql);
        }

        private boolean isBelowThreshold(SqlNode sql) {
            sql.accept(this);
            return orConditionCnt < KylinConfig.getInstanceFromEnv().getOptimizeTransformerConditionCountThreshold();
        }

        @Override
        protected void visitInSqlWhere(SqlNode node) {
            Queue<SqlNode> conditions = new LinkedList<>();
            conditions.add(node);
            while (!conditions.isEmpty()) {
                SqlNode cond = conditions.poll();
                if (cond instanceof SqlBasicCall) {
                    SqlBasicCall call = (SqlBasicCall) cond;
                    SqlKind kind = call.getOperator().getKind();
                    if (kind == SqlKind.AND) {
                        conditions.addAll(call.getOperandList());
                    } else if (kind == SqlKind.OR) {
                        conditions.addAll(call.getOperandList());
                        orConditionCnt++;
                    } else {
                        call.getOperator().acceptCall(this, call);
                    }
                }
            }
        }
    }

    static class MergeOrExprAndCollectInExprMatcher extends AbstractSqlVisitor {

        private final Map<SqlBasicCall, Action> toUpdateNodes = new HashMap<>();

        protected MergeOrExprAndCollectInExprMatcher(String originSql) {
            super(originSql);
        }

        private boolean isMergeableOrCollapsible(SqlNode sql) {
            sql.accept(this);
            return !toUpdateNodes.isEmpty();
        }

        private Map<SqlBasicCall, Action> getToUpdateNodes() {
            return toUpdateNodes;
        }

        @Override
        protected void visitInSqlWhere(SqlNode node) {
            Queue<SqlNode> conditions = new LinkedList<>();
            conditions.add(node);
            while (!conditions.isEmpty()) {
                SqlNode cond = conditions.poll();
                if (cond instanceof SqlBasicCall) {
                    SqlBasicCall call = (SqlBasicCall) cond;
                    SqlKind kind = call.getOperator().getKind();
                    if (kind == SqlKind.IN) {
                        collectInCommaList(call);
                    } else if (kind == SqlKind.AND) {
                        conditions.addAll(call.getOperandList());
                    } else if (kind == SqlKind.OR) {
                        mergeOrCondition(call);
                        conditions.addAll(call.getOperandList());
                    } else {
                        call.getOperator().acceptCall(this, call);
                    }
                }
            }
        }

        private void collectInCommaList(SqlBasicCall in) {
            assert in.getKind() == SqlKind.IN;
            if (in.operand(1) instanceof SqlNodeList) {
                toUpdateNodes.put(in, Action.MODIFY);
            } else if (in.operand(1) instanceof SqlSelect) {
                in.operand(1).accept(this);
            } else {
                throw new IllegalStateException("Unsupported sql syntax");
            }
        }

        private void mergeOrCondition(SqlBasicCall topOr) {
            while (true) {
                SqlNode topLeft = topOr.operand(0);
                SqlNode topRight = topOr.operand(1);
                if (topLeft.getKind() != SqlKind.OR) {
                    SqlNode tmp = topLeft;
                    topLeft = topRight;
                    topRight = tmp;
                }
                if (topLeft.getKind() != SqlKind.OR) {
                    return;
                }

                SqlBasicCall downOr = (SqlBasicCall) topLeft;
                SqlNode downLeft = downOr.operand(0);
                SqlNode downRight = downOr.operand(1);

                if (isMergeable(downLeft, downRight)) {
                    assert downLeft instanceof SqlBasicCall;
                    assert downRight instanceof SqlBasicCall;
                    mergeToLeft((SqlBasicCall) downLeft, (SqlBasicCall) downRight);
                    topOr.setOperand(0, downLeft);
                    topOr.setOperand(1, topRight);
                    toUpdateNodes.put((SqlBasicCall) downLeft, Action.MODIFY);
                    toUpdateNodes.put((SqlBasicCall) downRight, Action.DELETE);
                } else if (isMergeable(downLeft, topRight)) {
                    assert downLeft instanceof SqlBasicCall;
                    assert topRight instanceof SqlBasicCall;
                    mergeToLeft((SqlBasicCall) topRight, (SqlBasicCall) downLeft);
                    topOr.setOperand(0, topRight);
                    topOr.setOperand(1, downRight);
                    toUpdateNodes.put((SqlBasicCall) topRight, Action.MODIFY);
                    toUpdateNodes.put((SqlBasicCall) downLeft, Action.DELETE);
                } else if (isMergeable(topRight, downRight)) {
                    assert topRight instanceof SqlBasicCall;
                    assert downRight instanceof SqlBasicCall;
                    mergeToLeft((SqlBasicCall) topRight, (SqlBasicCall) downRight);
                    topOr.setOperand(0, topRight);
                    topOr.setOperand(1, downLeft);
                    toUpdateNodes.put((SqlBasicCall) topRight, Action.MODIFY);
                    toUpdateNodes.put((SqlBasicCall) downRight, Action.DELETE);
                } else {
                    return;
                }
            }
        }

        private boolean inCommaListOrIsEqual(SqlNode node) {
            return node.getKind() == SqlKind.IN && ((SqlBasicCall) node).operand(1) instanceof SqlNodeList
                    || node.getKind() == SqlKind.EQUALS;
        }

        private void mvIdentifierToLeft(SqlBasicCall call) {
            if (call.getKind() == SqlKind.EQUALS && !(call.operand(0) instanceof SqlIdentifier)
                    && call.operand(1) instanceof SqlIdentifier) {
                SqlNode tmp = call.operand(0);
                call.setOperand(0, call.operand(1));
                call.setOperand(1, tmp);
            }
        }

        private boolean isMergeable(SqlNode cond1, SqlNode cond2) {
            if (!inCommaListOrIsEqual(cond1) || !inCommaListOrIsEqual(cond2)) {
                return false;
            }
            assert cond1 instanceof SqlBasicCall;
            assert cond2 instanceof SqlBasicCall;
            mvIdentifierToLeft((SqlBasicCall) cond1);
            mvIdentifierToLeft((SqlBasicCall) cond2);
            return ((SqlBasicCall) cond1).operand(0).toString().equals(((SqlBasicCall) cond2).operand(0).toString());
        }

        private void mergeToLeft(SqlBasicCall leftCall, SqlBasicCall rightCall) {
            if (leftCall.getKind() == SqlKind.EQUALS) {
                leftCall.setOperator(SqlStdOperatorTable.IN);
                if (rightCall.getKind() == SqlKind.EQUALS) {
                    leftCall.setOperand(1, new SqlNodeList(Arrays.asList(leftCall.operand(1), rightCall.operand(1)),
                            leftCall.operand(1).getParserPosition()));
                } else if (rightCall.getKind() == SqlKind.IN) {
                    SqlNodeList nodeList = rightCall.operand(1);
                    nodeList.add(leftCall.operand(1));
                    leftCall.setOperand(1, nodeList);
                }
            } else if (leftCall.getKind() == SqlKind.IN) {
                if (rightCall.getKind() == SqlKind.EQUALS) {
                    ((SqlNodeList) leftCall.operand(1)).add(rightCall.operand(1));
                } else if (rightCall.getKind() == SqlKind.IN) {
                    ((SqlNodeList) leftCall.operand(1)).getList()
                            .addAll(((SqlNodeList) rightCall.operand(1)).getList());
                }
            }
        }
    }
}

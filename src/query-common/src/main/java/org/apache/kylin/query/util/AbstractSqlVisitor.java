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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.commons.collections.CollectionUtils;

public abstract class AbstractSqlVisitor extends SqlBasicVisitor<SqlNode> {
    public String originSql;

    protected AbstractSqlVisitor(String originSql) {
        this.originSql = originSql;
    }

    public static boolean isUnion(SqlCall call) {
        return call instanceof SqlBasicCall && call.getOperator().getKind() == SqlKind.UNION;
    }

    public static boolean isAs(SqlNode call) {
        return call instanceof SqlBasicCall && ((SqlBasicCall) call).getOperator().isName("AS", false);
    }

    public static boolean isSqlBasicCall(SqlNode call) {
        return call instanceof SqlBasicCall && !((SqlBasicCall) call).getOperandList().isEmpty();
    }

    @Override
    public SqlNode visit(SqlCall call) {
        if (isUnion(call)) {
            for (SqlNode operand : call.getOperandList()) {
                operand.accept(this);
            }
        }

        if (call instanceof SqlOrderBy) {
            visitInSqlOrderBy((SqlOrderBy) call);
        }
        if (call instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) call;
            visitInSqlSelect(sqlSelect);
        }

        if (call instanceof SqlWith) {
            sqlWithFound((SqlWith) call);
        }

        if (call instanceof SqlJoin) {
            visitInSqlJoin((SqlJoin) call);
        }

        return null;
    }

    private void visitInSqlOrderBy(SqlOrderBy orderBy) {
        SqlNodeList orderList = orderBy.orderList;
        visitInSqlNodeList(orderList);

        SqlNode limit = orderBy.fetch;
        visitInSqlNode(limit);

        SqlNode offset = orderBy.offset;
        visitInSqlNode(offset);

        SqlNode query = orderBy.query;
        query.accept(this);
    }

    private void visitInSqlSelect(SqlSelect sqlSelect) {
        visitInSelectList(sqlSelect);

        SqlNode from = sqlSelect.getFrom();

        if (from != null) {
            visitInSqlFrom(from);
        }

        visitInSqlWhere(sqlSelect.getWhere());

        visitInSqlNodeList(sqlSelect.getGroup());

        visitInSqlNode(sqlSelect.getHaving());

        visitInSqlNodeList(sqlSelect.getWindowList());

        SqlNode limit = sqlSelect.getFetch();
        visitInSqlNode(limit);

        visitInSqlNode(sqlSelect.getOffset());
    }

    protected void visitInSqlWhere(SqlNode where) {
        visitInSqlNode(where);
    }

    private void visitInSelectList(SqlSelect sqlSelect) {
        SqlNodeList selectNodeList = sqlSelect.getSelectList();
        List<SqlNode> selectList = selectNodeList.getList();
        for (SqlNode selectItem : selectList) {
            if (selectItem instanceof SqlWith) {
                sqlWithFound((SqlWith) selectItem);
            } else if (selectItem instanceof SqlOrderBy) {
                selectItem.accept(this);
            } else if (selectItem instanceof SqlSelect) {
                selectItem.accept(this);
            } else if (isAs(selectItem)) {
                visitInAsNode((SqlBasicCall) selectItem);
            } else if (isSqlBasicCall(selectItem)) {
                visitInSqlBasicCall((SqlBasicCall) selectItem);
            } else {
                visitInSqlNode(selectItem);
            }
        }
    }

    protected void visitInSqlFrom(SqlNode from) {
        if (from instanceof SqlWith) {
            sqlWithFound((SqlWith) from);
        } else if (isAs(from)) {
            visitInAsNode((SqlBasicCall) from);
        } else if (from instanceof SqlJoin) {
            SqlJoin join = (SqlJoin) from;
            visitInSqlJoin(join);
        } else {
            from.accept(this);
        }
    }

    protected void visitInSqlJoin(SqlJoin join) {
        visitInSqlNode(join.getLeft());
        visitInSqlNode(join.getRight());
        visitInSqlNode(join.getCondition());
    }

    protected void visitInAsNode(SqlBasicCall from) {
        SqlNode left = from.operand(0);
        visitInSqlNode(left);
    }

    protected void visitInSqlNode(SqlNode node) {
        if (node == null)
            return;
        if (node instanceof SqlWith) {
            sqlWithFound((SqlWith) node);
        } else if (node instanceof SqlDynamicParam) {
            questionMarkFound((SqlDynamicParam) node);
        } else if (node instanceof SqlNodeList) {
            visitInSqlNodeList((SqlNodeList) node);
        } else if (node instanceof SqlCase) {
            visitInSqlCase((SqlCase) node);
        } else if (isSqlBasicCall(node)) {
            visitInSqlBasicCall((SqlBasicCall) node);
        } else {
            node.accept(this);
        }
    }

    protected void visitInSqlBasicCall(SqlBasicCall call) {
        for (SqlNode ope : call.getOperandList()) {
            visitInSqlNode(ope);
        }
    }

    protected void visitInSqlNodeList(SqlNodeList sqlNodeList) {
        if (sqlNodeList == null)
            return;

        List<SqlNode> nodeList = sqlNodeList.getList();
        for (SqlNode node : nodeList) {
            visitInSqlNode(node);
        }
    }

    protected void visitInSqlCase(SqlCase sqlCase) {
        if (sqlCase == null) {
            return;
        }
        List<SqlNode> sqlNodes = sqlCase.getOperandList();
        if (CollectionUtils.isEmpty(sqlNodes)) {
            return;
        }
        for (SqlNode sqlNode : sqlNodes) {
            visitInSqlNode(sqlNode);
        }
    }

    protected void questionMarkFound(SqlDynamicParam questionMark) {
        //do something
    }

    protected void sqlWithFound(SqlWith sqlWith) {
        visitInSqlWithList(sqlWith.withList);
        SqlNode sqlWithQuery = sqlWith.body;
        sqlWithQuery.accept(this);
    }

    protected void visitInSqlWithList(SqlNodeList withList) {
        List<SqlNode> list = withList.getList();
        for (SqlNode node : list) {
            SqlWithItem withItem = (SqlWithItem) node;
            SqlNode query = withItem.query;
            visitInSqlNode(query);
        }
    }
}

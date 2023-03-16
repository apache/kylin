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
package org.apache.kylin.sdk.datasource.framework.conv;

import java.util.Map;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOverOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.kylin.common.util.Pair;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;

public class SqlNodeConverter extends SqlShuttle {

    private final ConvMaster convMaster;

    SqlNodeConverter(ConvMaster convMaster) {
        this.convMaster = convMaster;
    }

    @Override
    public SqlNode visit(SqlDataTypeSpec type) {
        SqlDataTypeSpec target = convertSqlDataTypeSpec(type);
        return target == null ? super.visit(type) : target;
    }

    @Override
    public SqlNode visit(SqlCall call) {
        SqlNode target = convertSqlCall(call);
        return target == null ? super.visit(call) : target;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
        SqlNode target = convertSqlIdentifier(id);
        return target == null ? super.visit(id) : target;
    }

    private SqlDataTypeSpec convertSqlDataTypeSpec(SqlDataTypeSpec typeSpec) {
        return convMaster.findTargetSqlDataTypeSpec(typeSpec);
    }

    private SqlNode convertSqlIdentifier(SqlIdentifier sqlIdentifier) {
        Pair<SqlNode, SqlNode> matched = convMaster.matchSqlFunc(sqlIdentifier);
        if (matched != null) {
            Preconditions.checkState(matched.getFirst() instanceof SqlIdentifier);
            return matched.getSecond();
        } else {
            return null;
        }
    }

    private SqlNode convertSqlCall(SqlCall sqlCall) {
        SqlOperator operator = sqlCall.getOperator();
        if (operator != null) {
            Pair<SqlNode, SqlNode> matched = convMaster.matchSqlFunc(sqlCall);

            if (matched != null) {
                Preconditions.checkState(matched.getFirst() instanceof SqlCall);
                SqlCall sourceTmpl = (SqlCall) matched.getFirst();

                Preconditions.checkState(sourceTmpl.operandCount() == sqlCall.operandCount());
                SqlNode targetTmpl = matched.getSecond();

                boolean isWindowCall = sourceTmpl.getOperator() instanceof SqlOverOperator;
                SqlParamsFinder sqlParamsFinder = SqlParamsFinder.newInstance(sourceTmpl, sqlCall, isWindowCall);
                return targetTmpl.accept(new SqlFuncFiller(sqlParamsFinder.getParamNodes(), isWindowCall));
            }
        }
        return null;
    }

    private class SqlFuncFiller extends SqlShuttle {

        private final Map<Integer, SqlNode> operands;

        private boolean isWindowCall = false;

        private SqlFuncFiller(final Map<Integer, SqlNode> operands) {
            this.operands = operands;
        }

        private SqlFuncFiller(final Map<Integer, SqlNode> operands, boolean isWindowCall) {
            this.operands = operands;
            this.isWindowCall = isWindowCall;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            String maybeParam = id.toString();
            int idx = ParamNodeParser.parseParamIdx(maybeParam);
            if (idx >= 0 && operands.containsKey(idx)) {
                SqlNode sqlNode = operands.get(idx);
                if (sqlNode instanceof SqlIdentifier) {
                    return sqlNode;
                } else {
                    return sqlNode.accept(SqlNodeConverter.this);
                }
            }
            return id;
        }

        @Override
        public SqlNode visit(SqlCall sqlCall) {
            return sqlCall instanceof SqlWindow && isWindowCall ? operands.get(1) : super.visit(sqlCall);
        }
    }
}

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

import static java.util.Objects.requireNonNull;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJdbcFunctionCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Iterables;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SqlFunctionUtilTest {

    @Test
    void testResolveCallIfNeed_NoNeed() {
        SqlJdbcFunctionCall.SimpleMakeCall simpleMakeCall = new SqlJdbcFunctionCall.SimpleMakeCall(
                SqlStdOperatorTable.ABS);
        SqlNodeList nodeList = new SqlNodeList(ImmutableList.of(SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO)),
                SqlParserPos.ZERO);
        SqlCall sqlCall = simpleMakeCall.createCall(SqlParserPos.ZERO, requireNonNull(nodeList, "thisOperands"));
        SqlCall resolveCall = SqlFunctionUtil.resolveCallIfNeed(sqlCall);
        Assertions.assertEquals(sqlCall, resolveCall);
    }

    @Test
    void testResolveCallIfNeed_UDF() {
        SqlIdentifier sqlIdentifier = new SqlIdentifier("myFunName", SqlParserPos.ZERO);
        SqlOperator fun = new SqlUnresolvedFunction(sqlIdentifier, null, null, null, null,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
        SqlNodeList nodeList = new SqlNodeList(ImmutableList.of(SqlLiteral.createBoolean(true, SqlParserPos.ZERO)),
                SqlParserPos.ZERO);
        SqlNode[] sqlNodes = Iterables.toArray(nodeList, SqlNode.class);
        SqlCall sqlCall = fun.createCall(SqlParserPos.ZERO, sqlNodes);
        Assertions.assertTrue(sqlCall.getOperator() instanceof SqlUnresolvedFunction);
        SqlCall resolveCall = SqlFunctionUtil.resolveCallIfNeed(sqlCall);
        Assertions.assertTrue(resolveCall.getOperator() instanceof SqlUnresolvedFunction);
    }

    @Test
    void testResolveCallIfNeed_builtIn() {
        SqlIdentifier sqlIdentifier = new SqlIdentifier("ABS", SqlParserPos.ZERO);
        SqlOperator fun = new SqlUnresolvedFunction(sqlIdentifier, ReturnTypes.ARG0, null,
                OperandTypes.NUMERIC_OR_INTERVAL, null, SqlFunctionCategory.NUMERIC);
        SqlNodeList nodeList = new SqlNodeList(ImmutableList.of(SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO)),
                SqlParserPos.ZERO);
        SqlNode[] sqlNodes = Iterables.toArray(nodeList, SqlNode.class);
        SqlCall sqlCall = fun.createCall(SqlParserPos.ZERO, sqlNodes);
        Assertions.assertTrue(sqlCall.getOperator() instanceof SqlUnresolvedFunction);
        SqlCall resolveCall = SqlFunctionUtil.resolveCallIfNeed(sqlCall);
        Assertions.assertFalse(resolveCall.getOperator() instanceof SqlUnresolvedFunction);
    }
}

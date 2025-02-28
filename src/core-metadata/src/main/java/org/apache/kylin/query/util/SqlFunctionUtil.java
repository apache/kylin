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

import java.util.Objects;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

public class SqlFunctionUtil {

    private SqlFunctionUtil() {
    }

    // see https://olapio.atlassian.net/browse/KE-42061
    public static SqlCall resolveCallIfNeed(SqlNode sqlNode) {
        SqlCall call = (SqlCall) sqlNode;
        if (call.getOperator() instanceof SqlUnresolvedFunction) {
            final SqlUnresolvedFunction unresolvedFunction = (SqlUnresolvedFunction) call.getOperator();
            final SqlOperator operator = SqlValidatorUtil.lookupSqlFunctionByID(SqlStdOperatorTable.instance(),
                    Objects.requireNonNull(unresolvedFunction.getSqlIdentifier()),
                    unresolvedFunction.getFunctionType());
            final SqlNode[] operands = call.getOperandList().toArray(SqlNode.EMPTY_ARRAY);
            // When use Kylin udf functions, operator is null, return call directly
            if (operator != null) {
                call = operator.createCall(call.getFunctionQuantifier(), call.getParserPosition(), operands);
            }
        }
        return call;
    }
}

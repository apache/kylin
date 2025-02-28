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

package org.apache.kylin.query.optrule;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.kylin.query.calcite.KylinSumSplitter;

public class OlapRuleUtils {

    private OlapRuleUtils() {
        throw new IllegalStateException("Utility class");
    }

    static boolean isDivide(RexNode expr) {
        if (SqlKind.DIVIDE == expr.getKind())
            return true;
        if (expr instanceof RexCall) {
            SqlOperator op = ((RexCall) expr).op;
            return op instanceof SqlUserDefinedFunction && "DIVIDE".equals(op.getName());
        }
        return false;
    }

    /**
     * Return replacement agg function for top agg build.
     * Beware that KYLIN_SUM is a special agg function of keeping decimal precision unchanged
     * and only suitable for small part of rules.
     *
     * @param aggCall original agg call
     * @return the replacement agg function
     */
    static SqlAggFunction getTopAggFunc(AggregateCall aggCall) {
        SqlKind kind = aggCall.getAggregation().getKind();
        switch (kind) {
            case COUNT:
                return SqlStdOperatorTable.SUM0;
            case SUM:
                return KylinSumSplitter.KYLIN_SUM;
            default:
                return aggCall.getAggregation();
        }
    }
}

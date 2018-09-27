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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.kylin.metadata.filter.CompareTupleFilter;

public class RexUtil {

    private RexUtil() {
        throw new IllegalStateException("Class RexUtil is an utility class !");
    }

    public static CompareTupleFilter.CompareResultType getCompareResultType(RexCall whenCall) {
        List<RexNode> operands = whenCall.getOperands();
        if (SqlKind.EQUALS == whenCall.getKind() && operands != null && operands.size() == 2) {
            if (operands.get(0).equals(operands.get(1))) {
                return CompareTupleFilter.CompareResultType.AlwaysTrue;
            }

            if (isConstant(operands.get(0)) && isConstant(operands.get(1))) {
                return CompareTupleFilter.CompareResultType.AlwaysFalse;
            }
        }
        return CompareTupleFilter.CompareResultType.Unknown;
    }

    public static boolean isConstant(RexNode rexNode) {
        if (rexNode instanceof RexLiteral) {
            return true;
        }

        if (rexNode instanceof RexCall && SqlKind.CAST.equals(rexNode.getKind())
                && ((RexCall) rexNode).getOperands().get(0) instanceof RexLiteral) {
            return true;
        }

        return false;
    }
}

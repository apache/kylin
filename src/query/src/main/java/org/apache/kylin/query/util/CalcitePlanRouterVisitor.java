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

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

import java.util.Optional;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.fun.SqlFloorFunction;
import org.apache.calcite.sql.type.NotConstant;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

/**
 * In constant query,
 * visit RexNode to find if any udf implement {@link org.apache.calcite.sql.type.NotConstant}
 *
 * Calcite does not support floor function properly, so route constant query with floor() to Sparder
 */
public class CalcitePlanRouterVisitor extends RexVisitorImpl<Boolean> {
    public CalcitePlanRouterVisitor() {
        super(true);
    }

    @Override
    public Boolean visitCall(RexCall call) {
        if (isConstantUdfContained(call) || isFloorFunctionContained(call))
            return TRUE;

        return call.getOperands().stream().anyMatch(operand -> defaultForEmpty(operand.accept(this)));
    }

    private static Boolean defaultForEmpty(Boolean result) {
        return Optional.ofNullable(result).orElse(FALSE);

    }

    private boolean isConstantUdfContained(RexCall call) {
        if ((call.getOperator() instanceof SqlUserDefinedFunction)
                && (((SqlUserDefinedFunction) (call.getOperator())).getFunction() instanceof ScalarFunctionImpl)) {

            SqlUserDefinedFunction sqlUserDefinedFunction = (SqlUserDefinedFunction) (call.getOperator());

            ScalarFunctionImpl scalarFunction = (ScalarFunctionImpl) (sqlUserDefinedFunction.getFunction());

            if (NotConstant.class.isAssignableFrom(scalarFunction.method.getDeclaringClass())) {
                return true;
            }
        }

        return false;
    }

    private boolean isFloorFunctionContained(RexCall call) {
        return call.getOperator() instanceof SqlFloorFunction;
    }
}

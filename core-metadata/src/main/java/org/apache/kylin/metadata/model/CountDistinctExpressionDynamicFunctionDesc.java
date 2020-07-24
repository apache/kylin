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

package org.apache.kylin.metadata.model;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.expression.TupleExpression;

public class CountDistinctExpressionDynamicFunctionDesc extends ExpressionDynamicFunctionDesc {

    public CountDistinctExpressionDynamicFunctionDesc(ParameterDesc parameter, TupleExpression tupleExpression) {
        super(parameter, FUNC_COUNT_DISTINCT, null, tupleExpression);
    }

    @Override
    public String getRewriteFieldName() {
        return "_KY_" + FUNC_COUNT_DISTINCT + "_" + tupleExpression.toString();
    }

    @Override
    protected FunctionDesc constructRuntimeFunction(TblColRef column) {
        return FunctionDesc.newInstance(FUNC_COUNT_DISTINCT, ParameterDesc.newInstance(column), null);
    }

    @Override
    protected void resetReturnType() {
        DataType returnType = null;
        for (FunctionDesc funcDesc : runtimeFuncMap.values()) {
            returnType = TupleExpression.referDataType(returnType, funcDesc.getReturnDataType());
        }
        setReturnDataType(returnType);
    }
}
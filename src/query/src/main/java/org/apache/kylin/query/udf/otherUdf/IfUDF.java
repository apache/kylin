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

package org.apache.kylin.query.udf.otherUdf;

import java.sql.Date;
import java.sql.Timestamp;

import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.UdfMethodNameImplementor;
import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.udf.UdfDef;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

public class IfUDF implements UdfDef {

    public static final SqlFunction OPERATOR = new SqlFunction(new SqlIdentifier("IF", SqlParserPos.ZERO),
            new SqlReturnTypeInference() {
                public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
                    return opBinding.getTypeFactory().leastRestrictive(
                            Lists.newArrayList(opBinding.getOperandType(1), opBinding.getOperandType(2)));
                }
            }, new SqlOperandTypeInference() {
                public void inferOperandTypes(SqlCallBinding callBinding, RelDataType returnType,
                        RelDataType[] operandTypes) {
                    RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
                    operandTypes[0] = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
                    operandTypes[1] = callBinding.getValidator().deriveType(callBinding.getScope(),
                            callBinding.operand(1));
                    operandTypes[2] = callBinding.getValidator().deriveType(callBinding.getScope(),
                            callBinding.operand(2));
                    if (operandTypes[1] instanceof BasicSqlType && operandTypes[2] instanceof BasicSqlType) {
                        BasicSqlType commonType = null;
                        commonType = (BasicSqlType) (operandTypes[1].getSqlTypeName() == SqlTypeName.NULL
                                ? operandTypes[2]
                                : operandTypes[1]);
                        if (commonType.getSqlTypeName() == SqlTypeName.NULL) { // make the type boolean if both exps are null
                            commonType = (BasicSqlType) typeFactory.createSqlType(SqlTypeName.BOOLEAN);
                        }
                        operandTypes[1] = callBinding.getTypeFactory().createTypeWithNullability(commonType, true);
                        operandTypes[2] = callBinding.getTypeFactory().createTypeWithNullability(commonType, true);
                    }
                }
            }, OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.ANY, SqlTypeFamily.ANY), null,
            SqlFunctionCategory.USER_DEFINED_FUNCTION);

    public static final CallImplementor IMPLEMENTOR = new UdfMethodNameImplementor("IF", IfUDF.class);

    public String IF(@Parameter(name = "exp") Boolean b, @Parameter(name = "str1") String expression1,
            @Parameter(name = "str2") String expression2) {
        return b ? expression1 : expression2;
    }

    public Integer IF(@Parameter(name = "exp") Boolean b, @Parameter(name = "num1") Integer expression1,
            @Parameter(name = "num2") Integer expression2) {
        return b ? expression1 : expression2;
    }

    public Double IF(@Parameter(name = "exp") Boolean b, @Parameter(name = "num1") Double expression1,
            @Parameter(name = "num2") Double expression2) {
        return b ? expression1 : expression2;
    }

    public Date IF(@Parameter(name = "exp") Boolean b, @Parameter(name = "date1") Date expression1,
            @Parameter(name = "date2") Date expression2) {
        return b ? expression1 : expression2;
    }

    public Timestamp IF(@Parameter(name = "exp") Boolean b, @Parameter(name = "date1") Timestamp expression1,
            @Parameter(name = "date2") Timestamp expression2) {
        return b ? expression1 : expression2;
    }

    public Boolean IF(@Parameter(name = "exp") Boolean b, @Parameter(name = "num1") Boolean expression1,
            @Parameter(name = "num2") Boolean expression2) {
        return b ? expression1 : expression2;
    }
}

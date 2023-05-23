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

package org.apache.kylin.query.udf.stringUdf;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.UdfMethodNameImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.fun.udf.UdfDef;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;

public class ConcatUDF implements UdfDef {

    private static final String FUNC_NAME = "CONCAT";

    private ConcatUDF() {
        throw new IllegalStateException("Utility class");
    }

    public static final SqlFunction OPERATOR = new SqlFunction(new SqlIdentifier(FUNC_NAME, SqlParserPos.ZERO),
            ReturnTypes.cascade(opBinding -> {
                int precision = opBinding.collectOperandTypes().stream().mapToInt(RelDataType::getPrecision).sum();
                return opBinding.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, precision);
            }, SqlTypeTransforms.TO_NULLABLE),
            (callBinding, returnType, operandTypes) -> Arrays.fill(operandTypes,
                    callBinding.getTypeFactory().createJavaType(Object.class)),
            OperandTypes.repeat(SqlOperandCountRanges.from(0), OperandTypes.ANY), null,
            SqlFunctionCategory.USER_DEFINED_FUNCTION);

    public static final CallImplementor IMPLEMENTOR = new UdfMethodNameImplementor(FUNC_NAME.toLowerCase(),
            ConcatUDF.class);

    public static String concat(Object... args) {
        if (args == null) {
            return null;
        }

        return Arrays.stream(args).map(String::valueOf).collect(Collectors.joining());
    }
}

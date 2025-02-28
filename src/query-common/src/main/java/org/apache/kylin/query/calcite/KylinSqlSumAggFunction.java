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

package org.apache.kylin.query.calcite;

import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Optionality;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

public class KylinSqlSumAggFunction extends SqlAggFunction {

    public static final SqlReturnTypeInference NON_WIDEN_AGG_SUM = opBinding -> {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        RelDataType returnType = opBinding.getOperandType(0);
        // TODO Use improved sum decimal precision by default in future
        boolean isImprovedSumDecimalPrecisionEnabled = KylinRelDataTypeSystem.getProjectConfig()
                .isImprovedSumDecimalPrecisionEnabled();
        if (!SqlTypeUtil.isDecimal(returnType) || !isImprovedSumDecimalPrecisionEnabled) {
            returnType = typeFactory.getTypeSystem().deriveSumType(typeFactory, returnType);
        }
        if (opBinding.getGroupCount() == 0 || opBinding.hasFilter()) {
            return typeFactory.createTypeWithNullability(returnType, true);
        } else {
            return returnType;
        }
    };

    //~ Instance fields --------------------------------------------------------

    /**
     * @deprecated field copied from {@link SqlSumAggFunction}
     */
    @Deprecated
    private final RelDataType type;

    //~ Constructors -----------------------------------------------------------

    public KylinSqlSumAggFunction(RelDataType type) {
        super("SUM", null, SqlKind.SUM, NON_WIDEN_AGG_SUM, null, OperandTypes.NUMERIC, SqlFunctionCategory.NUMERIC,
                false, false, Optionality.FORBIDDEN);
        this.type = type;
    }

    //~ Methods ----------------------------------------------------------------

    @SuppressWarnings("deprecation")
    @Override
    public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
        return ImmutableList.of(type);
    }

    /**
     * @deprecated method copied from {@link SqlSumAggFunction}
     * @return result rel data type
     */
    @Deprecated
    public RelDataType getType() {
        return type;
    }

    @SuppressWarnings("deprecation")
    @Override
    public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
        return type;
    }

    @Override
    public <T extends Object> @Nullable T unwrap(Class<T> clazz) {
        if (clazz == SqlSplittableAggFunction.class) {
            return clazz.cast(KylinSumSplitter.INSTANCE);
        }
        return super.unwrap(clazz);
    }

    @Override
    public SqlAggFunction getRollup() {
        return this;
    }
}

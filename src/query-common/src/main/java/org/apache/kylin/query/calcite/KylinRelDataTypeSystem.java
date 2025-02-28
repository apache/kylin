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

import java.util.Locale;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.metadata.project.NProjectManager;
import org.checkerframework.checker.nullness.qual.Nullable;

public class KylinRelDataTypeSystem extends RelDataTypeSystemImpl {

    private static final int MINIMUM_ADJUSTED_SCALE = 6;
    private static final int MAX_PRECISION = 38;
    private static final int MAX_SCALE = 38;
    public static final int SUM_RESULT_PRECISION_INCREASE = 10;

    @Override
    public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
        if (argumentType instanceof BasicSqlType) {
            switch (argumentType.getSqlTypeName()) {
            case DECIMAL:
                return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DECIMAL,
                        argumentType.getPrecision() + 4, argumentType.getScale() + 4), argumentType.isNullable());
            default:
                return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE),
                        argumentType.isNullable());
            }
        }
        return super.deriveAvgAggType(typeFactory, argumentType);
    }

    @Override
    public RelDataType deriveSumType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
        if (argumentType instanceof BasicSqlType) {
            switch (argumentType.getSqlTypeName()) {
            case INTEGER:
            case SMALLINT:
            case TINYINT:
                return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT),
                        argumentType.isNullable());
            case DECIMAL:
                // TODO Use improved sum decimal precision by default in future
                int precision = Math.max(19, argumentType.getPrecision());
                if (getProjectConfig().isImprovedSumDecimalPrecisionEnabled()) {
                    precision = Math.min(MAX_PRECISION, argumentType.getPrecision() + SUM_RESULT_PRECISION_INCREASE);
                }
                return typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, argumentType.getScale()),
                        argumentType.isNullable());
            default:
                break;
            }
        }
        return argumentType;
    }

    @Override
    public @Nullable RelDataType deriveDecimalDivideType(RelDataTypeFactory typeFactory, RelDataType type1,
            RelDataType type2) {
        if (SqlTypeUtil.isExactNumeric(type1) && SqlTypeUtil.isExactNumeric(type2)
                && (SqlTypeUtil.isDecimal(type1) || SqlTypeUtil.isDecimal(type2))) {
            // Java numeric will always have invalid precision/scale,
            // use its default decimal precision/scale instead.
            type1 = RelDataTypeFactoryImpl.isJavaType(type1) ? typeFactory.decimalOf(type1) : type1;
            type2 = RelDataTypeFactoryImpl.isJavaType(type2) ? typeFactory.decimalOf(type2) : type2;
            int p1 = type1.getPrecision();
            int p2 = type2.getPrecision();
            int s1 = type1.getScale();
            int s2 = type2.getScale();
            boolean decimalOperationsAllowPrecisionLoss = KylinConfig.getInstanceFromEnv()
                    .decimalOperationsAllowPrecisionLoss();
            RelDataType ret;
            if (decimalOperationsAllowPrecisionLoss) {
                int intDig = p1 - s1 + s2;
                int scale = Math.max(MINIMUM_ADJUSTED_SCALE, s1 + p2 + 1);
                int prec = intDig + scale;
                ret = adjustPrecisionScale(typeFactory, prec, scale);
            } else {
                int intDig = Math.min(MAX_SCALE, p1 - s1 + s2);
                int decDig = Math.min(MAX_SCALE, Math.max(6, s1 + p2 + 1));
                int diff = (intDig + decDig) - MAX_SCALE;
                if (diff > 0) {
                    decDig -= diff / 2 + 1;
                    intDig = MAX_SCALE - decDig;
                }
                ret = bounded(typeFactory, intDig + decDig, decDig);
            }
            return ret;
        }
        return null;
    }

    private void checkNegativeScale(int scale) {
        if (scale < 0 && !KylinConfig.getInstanceFromEnv().allowNegativeScaleOfDecimalEnabled()) {
            throw new KylinRuntimeException(String.format(Locale.ROOT,
                    "Negative scale is not allowed: %s. You can use %s=true to enable legacy mode to allow it.", scale,
                    KylinConfig.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED));
        }
    }

    private RelDataType adjustPrecisionScale(RelDataTypeFactory typeFactory, int precision, int scale) {
        // Assumptions:
        checkNegativeScale(scale);
        assert (precision >= scale);

        if (precision <= MAX_PRECISION) {
            // Adjustment only needed when we exceed max precision
            return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
        } else if (scale < 0) {
            // Decimal can have negative scale (SPARK-24468). In this case, we cannot allow a precision
            // loss since we would cause a loss of digits in the integer part.
            // In this case, we are likely to meet an overflow.
            return typeFactory.createSqlType(SqlTypeName.DECIMAL, MAX_PRECISION, scale);
        } else {
            // Precision/scale exceed maximum precision. Result must be adjusted to MAX_PRECISION.
            int intDigits = precision - scale;
            // If original scale is less than MINIMUM_ADJUSTED_SCALE, use original scale value; otherwise
            // preserve at least MINIMUM_ADJUSTED_SCALE fractional digits
            int minScaleValue = Math.min(scale, MINIMUM_ADJUSTED_SCALE);
            // The resulting scale is the maximum between what is available without causing a loss of
            // digits for the integer part of the decimal and the minimum guaranteed scale, which is
            // computed above
            int adjustedScale = Math.max(MAX_PRECISION - intDigits, minScaleValue);

            return typeFactory.createSqlType(SqlTypeName.DECIMAL, MAX_PRECISION, adjustedScale);
        }
    }

    private RelDataType bounded(RelDataTypeFactory typeFactory, int precision, int scale) {
        return typeFactory.createSqlType(SqlTypeName.DECIMAL, Math.min(precision, MAX_PRECISION),
                Math.min(scale, MAX_SCALE));
    }

    @Override
    public @Nullable RelDataType deriveDecimalMultiplyType(RelDataTypeFactory typeFactory, RelDataType type1,
            RelDataType type2) {
        if (SqlTypeUtil.isExactNumeric(type1) && SqlTypeUtil.isExactNumeric(type2)
                && (SqlTypeUtil.isDecimal(type1) || SqlTypeUtil.isDecimal(type2))) {
            // Java numeric will always have invalid precision/scale,
            // use its default decimal precision/scale instead.
            type1 = RelDataTypeFactoryImpl.isJavaType(type1) ? typeFactory.decimalOf(type1) : type1;
            type2 = RelDataTypeFactoryImpl.isJavaType(type2) ? typeFactory.decimalOf(type2) : type2;
            int p1 = type1.getPrecision();
            int p2 = type2.getPrecision();
            int s1 = type1.getScale();
            int s2 = type2.getScale();
            boolean decimalOperationsAllowPrecisionLoss = KylinConfig.getInstanceFromEnv()
                    .decimalOperationsAllowPrecisionLoss();

            RelDataType ret;
            int resultScale = s1 + s2;
            int resultPrecision = p1 + p2 + 1;
            if (decimalOperationsAllowPrecisionLoss) {
                ret = adjustPrecisionScale(typeFactory, resultPrecision, resultScale);
            } else {
                ret = bounded(typeFactory, resultPrecision, resultScale);
            }
            return ret;
        }

        return null;
    }

    /**
     * Hive support decimal with 38 digits, kylin should align
     *
     * @see org.apache.calcite.rel.type.RelDataTypeSystem
     * @see <a href="https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-DecimalsdecimalDecimals">Hive/LanguageManualTypes-Decimals</a>
     */
    @Override
    public int getMaxNumericPrecision() {
        return MAX_PRECISION;
    }

    @Override
    public boolean shouldConvertRaggedUnionTypesToVarying() {
        return true;
    }

    @Override
    public int getDefaultScale(SqlTypeName typeName) {
        switch (typeName) {
        case DECIMAL:
            return KapConfig.getInstanceFromEnv().defaultDecimalScale();
        default:
            return super.getDefaultScale(typeName);
        }
    }

    public static KylinConfig getProjectConfig() {
        return NProjectManager.getProjectConfig(QueryContext.current().getProject());
    }
}

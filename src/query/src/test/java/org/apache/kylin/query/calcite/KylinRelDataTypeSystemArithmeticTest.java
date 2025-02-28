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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class KylinRelDataTypeSystemArithmeticTest {
    private KylinRelDataTypeSystem typeSystem;
    private RelDataTypeFactory typeFactory;

    @BeforeEach
    public void beforeEach() {
        typeSystem = new KylinRelDataTypeSystem();
        typeFactory = new SqlTypeFactoryImpl(typeSystem);
    }

    @ParameterizedTest
    @CsvSource({ //
            "true,true,19,4,38,7", "false,true,19,4,38,8", "true,false,19,4,38,7", "false,false,19,4,38,8", //
            "true,true,38,2,38,4", "false,true,38,2,38,4", "true,false,38,2,38,4", "false,false,38,2,38,4", //
            "true,true,5,2,11,4", "false,true,5,2,11,4", "true,false,5,2,11,4", "false,false,5,2,11,4", //
            "true,true,5,-6,11,-12", "false,true,5,-6,11,-12", "true,false,5,-6,11,-12", "false,false,5,-6,11,-12" //
    })
    void testMultiply(boolean decimalOperationsAllowPrecisionLoss, boolean allowNegativeScaleOfDecimalEnabled,
            int precision, int scale, int precisionExpected, int scaleExpected) {
        try (MockedStatic<KylinConfig> configMockedStatic = Mockito.mockStatic(KylinConfig.class)) {
            KylinConfig config = Mockito.mock(KylinConfig.class);
            configMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(config);
            Mockito.when(config.decimalOperationsAllowPrecisionLoss()).thenReturn(decimalOperationsAllowPrecisionLoss);
            Mockito.when(config.allowNegativeScaleOfDecimalEnabled()).thenReturn(allowNegativeScaleOfDecimalEnabled);

            RelDataType relDataType1 = typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
            RelDataType relDataType2 = typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);

            RelDataType relDataType = typeSystem.deriveDecimalMultiplyType(typeFactory, relDataType1, relDataType2);
            if (decimalOperationsAllowPrecisionLoss && !allowNegativeScaleOfDecimalEnabled && precision == 5
                    && scale == -6) {
                Assertions.fail();
            }
            Assertions.assertNotNull(relDataType);
            int precisionActual = relDataType.getPrecision();
            Assertions.assertEquals(precisionExpected, precisionActual);
            final int scaleActual = relDataType.getScale();
            Assertions.assertEquals(scaleExpected, scaleActual);
        } catch (Exception e) {
            if (decimalOperationsAllowPrecisionLoss && !allowNegativeScaleOfDecimalEnabled && precision == 5
                    && scale == -6) {
                Assertions.assertInstanceOf(KylinRuntimeException.class, e);
                String expected = String.format("Negative scale is not allowed: %s. " //
                        + "You can use " //
                        + "kylin.storage.columnar.spark-conf.spark.sql.legacy.allowNegativeScaleOfDecimal=true " //
                        + "to enable legacy mode to allow it.", "-12");
                Assertions.assertEquals(expected, e.getMessage());
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ //
            "true,true,19,4,38,19", "false,true,19,4,38,21", "true,false,19,4,38,19", "false,false,19,4,38,21", //
            "true,true,38,2,38,6", "false,true,38,2,38,18", "true,false,38,2,38,6", "false,false,38,2,38,18", //
            "true,true,5,2,13,8", "false,true,5,2,13,8", "true,false,5,2,13,8", "false,false,5,2,13,8", //
            "true,true,5,-6,11,6", "false,true,5,-6,11,6", "true,false,5,-6,11,6", "false,false,5,-6,11,6" //
    })
    void testDivide(boolean decimalOperationsAllowPrecisionLoss, boolean allowNegativeScaleOfDecimalEnabled,
            int precision, int scale, int precisionExpected, int scaleExpected) {
        try (MockedStatic<KylinConfig> configMockedStatic = Mockito.mockStatic(KylinConfig.class)) {
            KylinConfig config = Mockito.mock(KylinConfig.class);
            configMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(config);
            Mockito.when(config.decimalOperationsAllowPrecisionLoss()).thenReturn(decimalOperationsAllowPrecisionLoss);
            Mockito.when(config.allowNegativeScaleOfDecimalEnabled()).thenReturn(allowNegativeScaleOfDecimalEnabled);

            RelDataType relDataType1 = typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
            RelDataType relDataType2 = typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);

            RelDataType relDataType = typeSystem.deriveDecimalDivideType(typeFactory, relDataType1, relDataType2);
            Assertions.assertNotNull(relDataType);
            int precisionActual = relDataType.getPrecision();
            Assertions.assertEquals(precisionExpected, precisionActual);
            final int scaleActual = relDataType.getScale();
            Assertions.assertEquals(scaleExpected, scaleActual);
        }
    }
}

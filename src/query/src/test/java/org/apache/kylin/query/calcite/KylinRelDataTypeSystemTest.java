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
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.query.schema.OlapTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@MetadataInfo
class KylinRelDataTypeSystemTest extends AbstractTestCase {

    @BeforeEach
    void beforeEach() {
        overwriteSystemProp("kylin.query.improved-sum-decimal-precision.enabled", "true");
    }

    @Test
    void testLegalDecimalType() {
        RelDataTypeSystem typeSystem = new KylinRelDataTypeSystem();
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(typeSystem);

        DataType dataType = DataType.getType("decimal(30, 10)");
        RelDataType relDataType = OlapTable.createSqlType(typeFactory, dataType, true);

        Assertions.assertTrue(relDataType instanceof BasicSqlType);
        Assertions.assertEquals(SqlTypeName.DECIMAL, relDataType.getSqlTypeName());
        Assertions.assertEquals(30, relDataType.getPrecision());
        Assertions.assertTrue(relDataType.getPrecision() <= typeSystem.getMaxNumericPrecision());
        Assertions.assertEquals(10, relDataType.getScale());
        Assertions.assertTrue(relDataType.getScale() <= typeSystem.getMaxNumericScale());
    }

    @Test
    void testSqlType() {
        RelDataTypeSystem typeSystem = new KylinRelDataTypeSystem();
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(typeSystem);
        Assertions.assertEquals("DECIMAL(19, 4)",
                OlapTable.createSqlType(typeFactory, DataType.getType("DECIMAL"), true).toString());
        Assertions.assertEquals("CHAR(255)",
                OlapTable.createSqlType(typeFactory, DataType.getType("CHAR"), true).toString());
        Assertions.assertEquals("VARCHAR(4096)",
                OlapTable.createSqlType(typeFactory, DataType.getType("VARCHAR"), true).toString());
        Assertions.assertEquals("INTEGER",
                OlapTable.createSqlType(typeFactory, DataType.getType("INTEGER"), true).toString());
        Assertions.assertEquals("TINYINT",
                OlapTable.createSqlType(typeFactory, DataType.getType("TINYINT"), true).toString());
        Assertions.assertEquals("SMALLINT",
                OlapTable.createSqlType(typeFactory, DataType.getType("SMALLINT"), true).toString());
        Assertions.assertEquals("BIGINT",
                OlapTable.createSqlType(typeFactory, DataType.getType("BIGINT"), true).toString());
        Assertions.assertEquals("FLOAT",
                OlapTable.createSqlType(typeFactory, DataType.getType("FLOAT"), true).toString());
        Assertions.assertEquals("DOUBLE",
                OlapTable.createSqlType(typeFactory, DataType.getType("DOUBLE"), true).toString());
        Assertions.assertEquals("DATE",
                OlapTable.createSqlType(typeFactory, DataType.getType("DATE"), true).toString());
        Assertions.assertEquals("TIMESTAMP(3)",
                OlapTable.createSqlType(typeFactory, DataType.getType("TIMESTAMP"), true).toString());
        Assertions.assertEquals("BOOLEAN",
                OlapTable.createSqlType(typeFactory, DataType.getType("BOOLEAN"), true).toString());
    }

    @Test
    void testIllegalDecimalType() {
        RelDataTypeSystem typeSystem = new KylinRelDataTypeSystem();
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(typeSystem);

        DataType dataType = DataType.getType("decimal(40, 10)");
        RelDataType relDataType = OlapTable.createSqlType(typeFactory, dataType, true);

        Assertions.assertTrue(relDataType instanceof BasicSqlType);
        Assertions.assertEquals(SqlTypeName.DECIMAL, relDataType.getSqlTypeName());
        Assertions.assertTrue(typeSystem.getMaxNumericPrecision() < 40);
        Assertions.assertEquals(relDataType.getPrecision(), typeSystem.getMaxNumericPrecision());
        Assertions.assertEquals(10, relDataType.getScale());
        Assertions.assertTrue(relDataType.getScale() <= typeSystem.getMaxNumericScale());
    }

    @Test
    void testDeriveDecimalSumType() {
        RelDataTypeSystem typeSystem = new KylinRelDataTypeSystem();
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(typeSystem);

        DataType dataType38 = DataType.getType("decimal(38, 10)");
        RelDataType relDataType38 = OlapTable.createSqlType(typeFactory, dataType38, true);
        RelDataType returnType38 = typeSystem.deriveSumType(typeFactory, relDataType38);
        Assertions.assertEquals(38, returnType38.getPrecision());
        Assertions.assertEquals(10, returnType38.getScale());

        DataType dataType40 = DataType.getType("decimal(40, 10)");
        RelDataType relDataType40 = OlapTable.createSqlType(typeFactory, dataType40, true);
        RelDataType returnType40 = typeSystem.deriveSumType(typeFactory, relDataType40);
        Assertions.assertEquals(38, returnType40.getPrecision());
        Assertions.assertEquals(10, returnType40.getScale());

        DataType dataType7 = DataType.getType("decimal(7, 10)");
        RelDataType relDataType7 = OlapTable.createSqlType(typeFactory, dataType7, true);
        RelDataType returnType7 = typeSystem.deriveSumType(typeFactory, relDataType7);
        Assertions.assertEquals(17, returnType7.getPrecision());
        Assertions.assertEquals(10, returnType7.getScale());
    }
}

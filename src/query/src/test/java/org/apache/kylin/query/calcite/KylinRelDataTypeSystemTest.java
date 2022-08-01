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
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.query.schema.OLAPTable;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KylinRelDataTypeSystemTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testLegalDecimalType() {
        RelDataTypeSystem typeSystem = new KylinRelDataTypeSystem();
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(typeSystem);

        DataType dataType = DataType.getType("decimal(30, 10)");
        RelDataType relDataType = OLAPTable.createSqlType(typeFactory, dataType, true);

        Assert.assertTrue(relDataType instanceof BasicSqlType);
        Assert.assertEquals(relDataType.getSqlTypeName(), SqlTypeName.DECIMAL);
        Assert.assertEquals(relDataType.getPrecision(), 30);
        Assert.assertTrue(relDataType.getPrecision() <= typeSystem.getMaxNumericPrecision());
        Assert.assertEquals(relDataType.getScale(), 10);
        Assert.assertTrue(relDataType.getScale() <= typeSystem.getMaxNumericScale());
    }

    @Test
    public void testSqlType() {
        RelDataTypeSystem typeSystem = new KylinRelDataTypeSystem();
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(typeSystem);
        Assert.assertEquals("DECIMAL(19, 4)",
                OLAPTable.createSqlType(typeFactory, DataType.getType("DECIMAL"), true).toString());
        Assert.assertEquals("CHAR(255)",
                OLAPTable.createSqlType(typeFactory, DataType.getType("CHAR"), true).toString());
        Assert.assertEquals("VARCHAR(4096)",
                OLAPTable.createSqlType(typeFactory, DataType.getType("VARCHAR"), true).toString());
        Assert.assertEquals("INTEGER",
                OLAPTable.createSqlType(typeFactory, DataType.getType("INTEGER"), true).toString());
        Assert.assertEquals("TINYINT",
                OLAPTable.createSqlType(typeFactory, DataType.getType("TINYINT"), true).toString());
        Assert.assertEquals("SMALLINT",
                OLAPTable.createSqlType(typeFactory, DataType.getType("SMALLINT"), true).toString());
        Assert.assertEquals("BIGINT",
                OLAPTable.createSqlType(typeFactory, DataType.getType("BIGINT"), true).toString());
        Assert.assertEquals("FLOAT", OLAPTable.createSqlType(typeFactory, DataType.getType("FLOAT"), true).toString());
        Assert.assertEquals("DOUBLE",
                OLAPTable.createSqlType(typeFactory, DataType.getType("DOUBLE"), true).toString());
        Assert.assertEquals("DATE", OLAPTable.createSqlType(typeFactory, DataType.getType("DATE"), true).toString());
        Assert.assertEquals("TIMESTAMP(3)",
                OLAPTable.createSqlType(typeFactory, DataType.getType("TIMESTAMP"), true).toString());
        Assert.assertEquals("BOOLEAN",
                OLAPTable.createSqlType(typeFactory, DataType.getType("BOOLEAN"), true).toString());
    }

    @Test
    public void testIllegalDecimalType() {
        RelDataTypeSystem typeSystem = new KylinRelDataTypeSystem();
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(typeSystem);

        DataType dataType = DataType.getType("decimal(40, 10)");
        RelDataType relDataType = OLAPTable.createSqlType(typeFactory, dataType, true);

        Assert.assertTrue(relDataType instanceof BasicSqlType);
        Assert.assertEquals(relDataType.getSqlTypeName(), SqlTypeName.DECIMAL);
        Assert.assertTrue(typeSystem.getMaxNumericPrecision() < 40);
        Assert.assertEquals(relDataType.getPrecision(), typeSystem.getMaxNumericPrecision());
        Assert.assertEquals(relDataType.getScale(), 10);
        Assert.assertTrue(relDataType.getScale() <= typeSystem.getMaxNumericScale());
    }

    @Test
    public void testDeriveDecimalSumType() {
        RelDataTypeSystem typeSystem = new KylinRelDataTypeSystem();
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(typeSystem);

        DataType dataType38 = DataType.getType("decimal(38, 10)");
        RelDataType relDataType38 = OLAPTable.createSqlType(typeFactory, dataType38, true);
        RelDataType returnType38 = typeSystem.deriveSumType(typeFactory, relDataType38);
        Assert.assertEquals(returnType38.getPrecision(), 38);
        Assert.assertEquals(returnType38.getScale(), 10);

        DataType dataType40 = DataType.getType("decimal(40, 10)");
        RelDataType relDataType40 = OLAPTable.createSqlType(typeFactory, dataType40, true);
        RelDataType returnType40 = typeSystem.deriveSumType(typeFactory, relDataType40);
        Assert.assertEquals(returnType40.getPrecision(), 38);
        Assert.assertEquals(returnType40.getScale(), 10);

        DataType dataType7 = DataType.getType("decimal(7, 10)");
        RelDataType relDataType7 = OLAPTable.createSqlType(typeFactory, dataType7, true);
        RelDataType returnType7 = typeSystem.deriveSumType(typeFactory, relDataType7);
        Assert.assertEquals(returnType7.getPrecision(), 19);
        Assert.assertEquals(returnType7.getScale(), 10);
    }
}

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
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.query.schema.OLAPTable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KylinRelDataTypeSystemTest extends LocalFileMetadataTestCase {
    
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
}

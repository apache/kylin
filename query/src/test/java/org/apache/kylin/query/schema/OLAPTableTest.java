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

package org.apache.kylin.query.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OLAPTableTest extends LocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testCreateSqlType() {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        DataType kylinDataType = DataType.getType("array<string>");

        RelDataType relDataType = OLAPTable.createSqlType(typeFactory, kylinDataType, true);
        Assert.assertTrue(relDataType instanceof ArraySqlType);
        Assert.assertEquals(SqlTypeName.ARRAY, relDataType.getSqlTypeName());
        Assert.assertTrue(relDataType.getComponentType() instanceof BasicSqlType);
        Assert.assertEquals(SqlTypeName.VARCHAR, relDataType.getComponentType().getSqlTypeName());
        Assert.assertTrue(relDataType.isNullable());

        kylinDataType = DataType.getType("array<>");
        boolean catchedEx = false;
        try {
            OLAPTable.createSqlType(typeFactory, kylinDataType, true);
        } catch (IllegalArgumentException ex) {
            Assert.assertEquals("Unrecognized data type array<>", ex.getMessage());
            catchedEx = true;
        }
        Assert.assertTrue(catchedEx);

        kylinDataType = DataType.getType("array<AAA>");
        catchedEx = false;
        try {
            OLAPTable.createSqlType(typeFactory, kylinDataType, true);
        } catch (IllegalArgumentException ex) {
            Assert.assertTrue(ex.getMessage().startsWith("bad data type -- aaa"));
            catchedEx = true;
        }
        Assert.assertTrue(catchedEx);
    }
}

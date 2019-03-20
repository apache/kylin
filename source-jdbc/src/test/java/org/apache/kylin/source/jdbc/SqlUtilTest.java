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
package org.apache.kylin.source.jdbc;

import java.sql.Types;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SqlUtilTest {

    @Test
    public void testJdbcTypetoKylinDataType() {
        this.getClass().getClassLoader().toString();
        assertEquals("double", SqlUtil.jdbcTypeToKylinDataType(Types.FLOAT));
        assertEquals("varchar", SqlUtil.jdbcTypeToKylinDataType(Types.NVARCHAR));
        assertEquals("any", SqlUtil.jdbcTypeToKylinDataType(Types.ARRAY));
        assertEquals("integer", SqlUtil.jdbcTypeToKylinDataType((4)));
        assertEquals("smallint", SqlUtil.jdbcTypeToKylinDataType((5)));
        assertEquals("tinyint", SqlUtil.jdbcTypeToKylinDataType((-6)));
        assertEquals("char", SqlUtil.jdbcTypeToKylinDataType((1)));
        assertEquals("decimal", SqlUtil.jdbcTypeToKylinDataType((2)));
        assertEquals("varchar", SqlUtil.jdbcTypeToKylinDataType((-1)));
        assertEquals("byte", SqlUtil.jdbcTypeToKylinDataType((-2)));
        assertEquals("any", SqlUtil.jdbcTypeToKylinDataType((-1720774701)));
        assertEquals("boolean", SqlUtil.jdbcTypeToKylinDataType((-7)));
        assertEquals("timestamp", SqlUtil.jdbcTypeToKylinDataType((93)));
        assertEquals("time", SqlUtil.jdbcTypeToKylinDataType((92)));
        assertEquals("date", SqlUtil.jdbcTypeToKylinDataType((91)));
        assertEquals("bigint", SqlUtil.jdbcTypeToKylinDataType((-5)));
    }

    @Test
    public void testIsPrecisionApplicable() {
        assertFalse(SqlUtil.isPrecisionApplicable("boolean"));
        assertTrue(SqlUtil.isPrecisionApplicable("varchar"));
    }

    @Test
    public void testIsScaleApplicable() {
        assertFalse(SqlUtil.isScaleApplicable("varchar"));
        assertTrue(SqlUtil.isScaleApplicable("decimal"));
    }

}

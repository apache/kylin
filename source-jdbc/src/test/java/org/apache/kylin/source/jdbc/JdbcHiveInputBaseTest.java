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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.SourceDialect;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JdbcHiveInputBaseTest extends LocalFileMetadataTestCase {

    @BeforeClass
    public static void setupClass() throws SQLException {
        staticCreateTestMetadata();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.setProperty("kylin.source.hive.quote-enabled", "true");
    }

    @Test
    public void testFetchValue() {
        Map<String, String> map = new HashMap<>();
        String guess = JdbcHiveInputBase.fetchValue("DB_1", "TB_2", "COL_3", map);

        // not found, return input value
        assertEquals("DB_1.TB_2.COL_3", guess);
        map.put("DB_1.TB_2.COL_3", "Db_1.Tb_2.Col_3");

        guess = JdbcHiveInputBase.fetchValue("DB_1", "TB_2", "COL_3", map);
        // found, return cached value
        assertEquals("Db_1.Tb_2.Col_3", guess);
    }

    @Test
    public void testQuoteIdentifier() {
        String guess = JdbcHiveInputBase.quoteIdentifier("Tbl1.Col1", SourceDialect.MYSQL);
        assertEquals("`Tbl1`.`Col1`", guess);
        guess = JdbcHiveInputBase.quoteIdentifier("Tbl1.Col1", SourceDialect.MSSQL);
        assertEquals("[Tbl1].[Col1]", guess);
    }

    @AfterClass
    public static void clenup() {
        staticCleanupTestMetadata();
    }
}

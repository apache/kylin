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

package org.apache.kylin.query.engine;

import java.sql.SQLException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.query.QueryExtension;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueryExecColumnMetaTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        // Use default Factory for Open Core
        QueryExtension.setFactory(new QueryExtension.Factory());
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
        // Unset Factory for Open Core
        QueryExtension.setFactory(null);
    }

    public static String[] sqls = { "SELECT \"pRICE\",\n" + "\"PRice\"\n" + "FROM \"TEST_KYLIN_FACT\"\n",
            "SELECT* FROM (\n" + "SELECT \"pRICE\",\n" + "        \"PRice\"\n" + "    FROM \"TEST_KYLIN_FACT\"\n"
                    + ")\n",
            "SELECT* FROM (\n" + "SELECT \"PRICE\" AS \"pRICE\",\n" + "        \"PRICE\" AS \"PRice\"\n"
                    + "    FROM \"TEST_KYLIN_FACT\"\n" + ")\n",
            "SELECT * FROM (SELECT* FROM (\n" + "SELECT \"PRICE\" AS \"pRICE\",\n" + "        \"PRICE\" AS \"PRice\"\n"
                    + "    FROM \"TEST_KYLIN_FACT\"\n" + ") ORDER BY 2,1)\n" };

    public static String[][] expectedColumnNamesList = { { "pRICE", "PRice" }, { "pRICE", "PRice" },
            { "pRICE", "PRice" }, { "pRICE", "PRice" } };

    @Test
    public void testColumnNames() {
        assert sqls.length == expectedColumnNamesList.length;

        String expectedError = "Column 'pRICE' is ambiguous";
        for (String sql : sqls) {
            try {
                new QueryExec("newten", KylinConfig.getInstanceFromEnv()).getColumnMetaData(sql);
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains(expectedError));
            }
        }
    }

}

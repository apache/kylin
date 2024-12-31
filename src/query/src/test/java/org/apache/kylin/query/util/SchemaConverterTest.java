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
package org.apache.kylin.query.util;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import org.apache.kylin.common.QueryContext;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.table.InternalTableDesc;
import org.apache.kylin.metadata.table.InternalTableManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MetadataInfo()
public class SchemaConverterTest {

    SchemaConverter converter = new SchemaConverter();

    @AfterEach
    public void teardown() {
        QueryContext.current().getQueryTagInfo().setPushdown(false);
    }

    @Test
    void testCatalogConvert() {
        String sql = "select t1.id t_id from SSB.P_LINEORDER t1 left join \"SSB\".\"PART\" on t1.PARTKEY = PART.PARTKEY "
                + "union all select * from SSB.LINEORDER union all select * from \"DEFAULT\".TEST_COUNTRY t";

        String expectedSql = "select t1.id t_id from \"INTERNAL_CATALOG\".\"default\".\"SSB\".\"P_LINEORDER\" t1 "
                + "left join \"INTERNAL_CATALOG\".\"default\".\"SSB\".\"PART\" on t1.PARTKEY = PART.PARTKEY "
                + "union all select * from \"INTERNAL_CATALOG\".\"default\".\"SSB\".\"LINEORDER\" "
                + "union all select * from \"INTERNAL_CATALOG\".\"default\".\"DEFAULT\".\"TEST_COUNTRY\" t";
        Assertions.assertEquals(sql, converter.convert(sql, "default", null));

        getTestConfig().setProperty("kylin.internal-table-enabled", "true");

        prepareInternalTable();

        Assertions.assertEquals(sql, converter.convert(sql, "default", null));

        QueryContext.current().getQueryTagInfo().setPushdown(true);

        checkAsyncQuery(sql, expectedSql);

        Assertions.assertEquals(expectedSql, converter.convert(sql, "default", null));
    }

    private void checkAsyncQuery(String sql, String expectedSql) {
        QueryContext.current().getQueryTagInfo().setAsyncQuery(true);
        getTestConfig().setProperty("kylin.query.unique-async-query-yarn-queue-enabled", "true");
        getTestConfig().setProperty("kylin.unique-async-query.gluten.enabled", "false");
        Assertions.assertEquals(sql, converter.convert(sql, "default", null));

        QueryContext.current().getQueryTagInfo().setAsyncQuery(false);
        Assertions.assertEquals(expectedSql, converter.convert(sql, "default", null));

        getTestConfig().setProperty("kylin.query.unique-async-query-yarn-queue-enabled", "false");
        Assertions.assertEquals(expectedSql, converter.convert(sql, "default", null));

        getTestConfig().setProperty("kylin.unique-async-query.gluten.enabled", "true");
        Assertions.assertEquals(expectedSql, converter.convert(sql, "default", null));

        QueryContext.current().getQueryTagInfo().setAsyncQuery(true);
        getTestConfig().setProperty("kylin.query.unique-async-query-yarn-queue-enabled", "false");
        Assertions.assertEquals(expectedSql, converter.convert(sql, "default", null));

        getTestConfig().setProperty("kylin.query.unique-async-query-yarn-queue-enabled", "true");
        getTestConfig().setProperty("kylin.unique-async-query.gluten.enabled", "true");
        Assertions.assertEquals(expectedSql, converter.convert(sql, "default", null));
    }

    @Test
    void testConvertWithDefaultSchema() {
        getTestConfig().setProperty("kylin.internal-table-enabled", "true");

        prepareInternalTable();

        QueryContext.current().getQueryTagInfo().setPushdown(true);
        String sql1 = "select TRANS_ID from test_kylin_fact";
        String result1 = converter.convert(sql1, "default", "default");
        String expectedSql1 = "select TRANS_ID "
                + "from \"INTERNAL_CATALOG\".\"default\".\"DEFAULT\".\"TEST_KYLIN_FACT\"";
        Assertions.assertEquals(expectedSql1, result1);

        String sql2 = "with t1 as (select TRANS_ID from test_kylin_fact) select * from t1";
        String result2 = converter.convert(sql2, "default", "default");
        String expectedSql2 = "with t1 as (select TRANS_ID "
                + "from \"INTERNAL_CATALOG\".\"default\".\"DEFAULT\".\"TEST_KYLIN_FACT\")" + " select * from t1";
        Assertions.assertEquals(expectedSql2, result2);

        getTestConfig().setProperty("kylin.source.name-case-sensitive-enabled", "true");
        String sql3 = "with t1 as (select TRANS_ID from test_kylin_fact) select * from t1";
        String result3 = converter.convert(sql3, "default", "default");
        String expectedSql3 = "with t1 as (select TRANS_ID from test_kylin_fact) select * from t1";
        Assertions.assertEquals(expectedSql3, result3);
        Assertions.assertTrue(QueryContext.current().getQueryTagInfo().isErrInterrupted());
        Assertions.assertEquals("Table default.test_kylin_fact is not an internal table.",
                QueryContext.current().getQueryTagInfo().getInterruptReason());

        getTestConfig().setProperty("kylin.source.name-case-sensitive-enabled", "false");
        String sql4 = "with t1 as (select TRANS_ID from test_kylin_fact), t2 as (select TRANS_ID from t1) "
                + "select * from t2";
        String result4 = converter.convert(sql4, "default", "default");
        String expectedSql4 = "with t1 as (select TRANS_ID "
                + "from \"INTERNAL_CATALOG\".\"default\".\"DEFAULT\".\"TEST_KYLIN_FACT\"), "
                + "t2 as (select TRANS_ID from t1) select * from t2";
        Assertions.assertEquals(expectedSql4, result4);
        QueryContext.current().getQueryTagInfo().setPushdown(false);
    }

    private void prepareInternalTable() {
        InternalTableManager innerTableMgr = InternalTableManager.getInstance(getTestConfig(), "default");
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "default");
        for (TableDesc tableDesc : tableMgr.listAllTables()) {
            innerTableMgr.createInternalTable(new InternalTableDesc(tableDesc));
        }
    }
}

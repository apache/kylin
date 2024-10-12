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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MetadataInfo()
public class SchemaConverterTest {

    SchemaConverter converter = new SchemaConverter();

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

        InternalTableManager innerTableMgr = InternalTableManager.getInstance(getTestConfig(), "default");
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "default");
        for (TableDesc tableDesc : tableMgr.listAllTables()) {
            innerTableMgr.createInternalTable(new InternalTableDesc(tableDesc));
        }

        Assertions.assertEquals(sql, converter.convert(sql, "default", null));

        QueryContext.current().getQueryTagInfo().setPushdown(true);

        QueryContext.current().getQueryTagInfo().setAsyncQuery(true);
        getTestConfig().setProperty("kylin.query.unique-async-query-yarn-queue-enabled", "true");
        Assertions.assertEquals(sql, converter.convert(sql, "default", null));

        QueryContext.current().getQueryTagInfo().setAsyncQuery(false);
        Assertions.assertEquals(expectedSql, converter.convert(sql, "default", null));

        getTestConfig().setProperty("kylin.query.unique-async-query-yarn-queue-enabled", "false");
        Assertions.assertEquals(expectedSql, converter.convert(sql, "default", null));

        QueryContext.current().getQueryTagInfo().setAsyncQuery(true);
        Assertions.assertEquals(expectedSql, converter.convert(sql, "default", null));

        Assertions.assertEquals(expectedSql, converter.convert(sql, "default", null));
    }
}

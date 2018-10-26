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
package org.apache.kylin.source.jdbc.extensible;

import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.SourceManager;
import org.junit.Assert;
import org.junit.Test;

import java.util.Locale;

public class JdbcTableTest extends TestBase {

    @Test
    public void testBasics() throws Exception {
        TableMetadataManager tblManager = TableMetadataManager.getInstance(getTestConfig());
        TableDesc tblDesc = tblManager.getTableDesc("test_kylin_fact", "default");
        IReadableTable table = SourceManager.getSource(new JdbcSourceTest.JdbcSourceAware())
                .createReadableTable(tblDesc, null);

        // test TableReader
        try (IReadableTable.TableReader reader = table.getReader()) {
            Assert.assertTrue(reader instanceof JdbcTableReader);
            Assert.assertTrue(table instanceof JdbcTable);

            Assert.assertTrue(reader.next());
            String[] row = reader.getRow();
            Assert.assertNotNull(row);
            Assert.assertEquals(tblDesc.getColumnCount(), row.length);
        }

        // test basics
        Assert.assertTrue(table.exists());

        IReadableTable.TableSignature sign = table.getSignature();
        Assert.assertNotNull(sign);
        Assert.assertEquals(String.format(Locale.ROOT, "%s.%s", tblDesc.getDatabase(), tblDesc.getName()), sign.getPath());
        Assert.assertTrue(sign.getLastModifiedTime() > 0);
    }

}

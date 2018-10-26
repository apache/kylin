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

import java.util.List;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceManager;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class JdbcExplorerTest extends TestBase {
    private static ISourceMetadataExplorer explorer = null;

    @BeforeClass
    public static void setUp() throws Exception {
        TestBase.setUp();
        explorer = SourceManager.getSource(new JdbcSourceTest.JdbcSourceAware()).getSourceMetadataExplorer();
    }

    @Rule
    public ExpectedException validateSQLInvalidEx = ExpectedException.none();

    @Test
    public void testListDatabases() throws Exception {
        List<String> dbList = explorer.listDatabases();
        Assert.assertTrue(dbList.size() >= 3);
        Assert.assertTrue(dbList.contains("EDW"));
        Assert.assertTrue(dbList.contains("DEFAULT"));
    }

    @Test
    public void testListTables() throws Exception {
        List<String> tblList = explorer.listTables("DEFAULT");
        Assert.assertTrue(tblList.size() >= 3);
        Assert.assertTrue(tblList.contains("TEST_KYLIN_FACT"));
        Assert.assertTrue(tblList.contains("TEST_ACCOUNT"));
        Assert.assertTrue(tblList.contains("TEST_COUNTRY"));
    }

    @Test
    public void testValidateSql() throws Exception {
        explorer.validateSQL("select 1");
        validateSQLInvalidEx.expect(Exception.class);
        explorer.validateSQL("select");
    }


    @Test
    public void testGetRelatedKylinResources() {
        Assert.assertTrue(explorer.getRelatedKylinResources(null).isEmpty());
    }

    @Test
    public void testLoadTableMetadata() throws Exception {
        Pair<TableDesc, TableExtDesc> pair = explorer.loadTableMetadata("DEFAULT", "TEST_KYLIN_FACT", "DEFAULT");
        Assert.assertNotNull(pair.getFirst());
        Assert.assertNotNull(pair.getSecond());

        TableDesc tblDesc = pair.getFirst();
        TableExtDesc tblExtDesc = pair.getSecond();
        Assert.assertEquals("TEST_KYLIN_FACT", tblDesc.getName());
        Assert.assertEquals("TABLE", tblDesc.getTableType());
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", tblDesc.getIdentity());
        Assert.assertEquals("DEFAULT", tblDesc.getDatabase());
        Assert.assertEquals("DEFAULT", tblDesc.getProject());
        Assert.assertEquals(tblDesc.getIdentity(), tblExtDesc.getIdentity());
        Assert.assertEquals(tblDesc.getProject(), tblExtDesc.getProject());

        ColumnDesc[] columnDescs = tblDesc.getColumns();
        Assert.assertEquals(tblDesc.getColumnCount(), columnDescs.length);
        Assert.assertNotNull(columnDescs[0].getName());
        Assert.assertNotNull(columnDescs[0].getDatatype());
        Assert.assertNotNull(columnDescs[0].getType());
        Assert.assertNotNull(columnDescs[0].getId());
    }

    @Test
    public void testEvalQueryMetadata() {
        ColumnDesc[] columnDescs = explorer
                .evalQueryMetadata("select cal_dt, count(*) as cnt from DEFAULT.test_kylin_fact group by cal_dt");
        Assert.assertNotNull(columnDescs);
        Assert.assertEquals(2, columnDescs.length);
        Assert.assertEquals("date", columnDescs[0].getDatatype());
        Assert.assertEquals("CAL_DT", columnDescs[0].getName());
        Assert.assertEquals("bigint", columnDescs[1].getDatatype());
        Assert.assertEquals("CNT", columnDescs[1].getName());
    }
}

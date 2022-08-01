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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.metadata.project.NProjectManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(MockitoJUnitRunner.class)
public class RawSqlTest {

    private static final String SQL = "select /*+ MODEL_PRIORITY(model1, model2) */ 'col1-;1', col2 -- comment1;\n"
            + "from table -- comment2\n" + "/* comment3\n" + "   comment4;\n" + "   comment5\n" + "*/\n"
            + "limit /* comment6 */ 10; -- comment7;";

    private static final String STATEMENT_STRING = "select /*+ MODEL_PRIORITY(model1, model2) */ 'col1-;1', col2\n"
            + "from table\n" + "limit 10";

    private static final String FULL_TEXT_STRING = "select /*+ MODEL_PRIORITY(model1, model2) */ 'col1-;1', col2 -- comment1;\n"
            + "from table -- comment2\n" + "/* comment3\n" + "   comment4;\n" + "   comment5\n" + "*/\n"
            + "limit /* comment6 */ 10 -- comment7;";

    private static final String SQL2 = "select * from table1";

    private RawSql rawSql;
    private RawSql rawSql2;

    @Before
    public void setUp() throws Exception {
        rawSql = new RawSqlParser(SQL).parse();
        rawSql2 = new RawSqlParser(SQL2).parse();
    }

    @Test
    public void testGetStatementString() {
        assertNull(ReflectionTestUtils.getField(rawSql, "statementStringCache"));
        assertEquals(STATEMENT_STRING, rawSql.getStatementString());
        assertNotNull(ReflectionTestUtils.getField(rawSql, "statementStringCache"));
    }

    @Test
    public void testGetFullTextString() {
        assertNull(ReflectionTestUtils.getField(rawSql, "fullTextStringCache"));
        assertEquals(FULL_TEXT_STRING, rawSql.getFullTextString());
        assertNotNull(ReflectionTestUtils.getField(rawSql, "fullTextStringCache"));
    }

    @Test
    public void testAutoAppendLimit() {
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class)) {
            KylinConfig kylinConfig = mock(KylinConfig.class);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(kylinConfig);

            when(kylinConfig.getForceLimit()).thenReturn(-1);
            rawSql2.autoAppendLimit(kylinConfig, 20);
            assertEquals(SQL2 + "\nLIMIT 20", rawSql2.getStatementString());

            rawSql2.autoAppendLimit(kylinConfig, 20, 10);
            assertEquals(SQL2 + "\nLIMIT 20\nOFFSET 10", rawSql2.getStatementString());
        }
    }

    @Test
    public void testAutoAppendLimitForceLimit() {
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class)) {
            KylinConfig kylinConfig = mock(KylinConfig.class);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(kylinConfig);

            when(kylinConfig.getForceLimit()).thenReturn(15);
            rawSql2.autoAppendLimit(kylinConfig, 0, 0);
            assertEquals(SQL2 + "\nLIMIT 15", rawSql2.getStatementString());
        }
    }

    @Test
    public void testClearCache() {
        assertNull(ReflectionTestUtils.getField(rawSql, "statementStringCache"));
        rawSql.getStatementString();
        assertNotNull(ReflectionTestUtils.getField(rawSql, "statementStringCache"));

        assertNull(ReflectionTestUtils.getField(rawSql, "fullTextStringCache"));
        rawSql.getFullTextString();
        assertNotNull(ReflectionTestUtils.getField(rawSql, "fullTextStringCache"));

        ReflectionTestUtils.invokeMethod(rawSql, "clearCache");
        assertNull(ReflectionTestUtils.getField(rawSql, "statementStringCache"));
        assertNull(ReflectionTestUtils.getField(rawSql, "fullTextStringCache"));
    }

    @Test
    public void testIsSelectStatement() throws Exception {
        assertEquals(Boolean.TRUE, ReflectionTestUtils.invokeMethod(rawSql, "isSelectStatement"));

        RawSql withRawSql = new RawSqlParser("( with table_tmp as (select col1 from table1))").parse();
        assertEquals(Boolean.TRUE, ReflectionTestUtils.invokeMethod(withRawSql, "isSelectStatement"));

        RawSql explainRawSql = new RawSqlParser("explain select col1 from table1").parse();
        assertEquals(Boolean.TRUE, ReflectionTestUtils.invokeMethod(explainRawSql, "isSelectStatement"));

        RawSql createRawSql = new RawSqlParser("create table xxx as (select col1, col2 from table1)").parse();
        assertEquals(Boolean.FALSE, ReflectionTestUtils.invokeMethod(createRawSql, "isSelectStatement"));
    }

    @Test
    public void testSqlLimitAndForceLimit() throws ParseException {
        RawSql tmpSql = new RawSqlParser("select * from table1 limit 10").parse();
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class)) {
            KylinConfig kylinConfig = mock(KylinConfig.class);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(kylinConfig);

            when(kylinConfig.getForceLimit()).thenReturn(15);
            tmpSql.autoAppendLimit(kylinConfig, 0);
            assertEquals("select * from table1 limit 10", tmpSql.getStatementString());
        }
    }

    @Test
    public void testProjectForceLimitEnabled() throws ParseException {
        RawSql tmpSql = new RawSqlParser("select * from table1").parse();
        try (MockedStatic<NProjectManager> nProjectManagerMockedStatic = Mockito.mockStatic(NProjectManager.class)) {
            NProjectManager projectManager = Mockito.mock(NProjectManager.class);
            nProjectManagerMockedStatic.when(() -> NProjectManager.getInstance(Mockito.any()))
                    .thenReturn(projectManager);
            KylinConfigExt kylinConfigExt = Mockito.mock(KylinConfigExt.class);
            when(kylinConfigExt.getForceLimit()).thenReturn(14);
            tmpSql.autoAppendLimit(kylinConfigExt, 0);
            assertEquals("select * from table1" + "\n" + "LIMIT 14", tmpSql.getStatementString());
        }
    }

    @Test
    public void testMaxResultRowsEnabled() throws ParseException {
        RawSql tmpSql = new RawSqlParser("select * from table1").parse();
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class)) {
            KylinConfig kylinConfig = mock(KylinConfig.class);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(kylinConfig);

            when(kylinConfig.getMaxResultRows()).thenReturn(15);
            when(kylinConfig.getForceLimit()).thenReturn(14);
            tmpSql.autoAppendLimit(kylinConfig, 16);
            assertEquals("select * from table1" + "\n" + "LIMIT 15", tmpSql.getStatementString());
        }
    }

    @Test
    public void testCompareMaxResultRowsAndLimit() throws ParseException {
        RawSql tmpSql = new RawSqlParser("select * from table1").parse();
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class)) {
            KylinConfig kylinConfig = mock(KylinConfig.class);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(kylinConfig);

            when(kylinConfig.getMaxResultRows()).thenReturn(15);
            when(kylinConfig.getForceLimit()).thenReturn(14);
            tmpSql.autoAppendLimit(kylinConfig, 13);
            assertEquals("select * from table1" + "\n" + "LIMIT 13", tmpSql.getStatementString());
        }
    }
}

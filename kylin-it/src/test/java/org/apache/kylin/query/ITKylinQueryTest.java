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

package org.apache.kylin.query;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.gridtable.GTScanSelfTerminatedException;
import org.apache.kylin.gridtable.StorageSideBehavior;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.enumerator.OLAPQuery;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;
import org.apache.kylin.query.schema.OLAPSchemaFactory;
import org.apache.kylin.storage.hbase.HBaseStorage;
import org.apache.kylin.storage.hbase.cube.v1.coprocessor.observer.ObserverEnabler;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Maps;

public class ITKylinQueryTest extends KylinTestBase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {
        printInfo("setUp in ITKylinQueryTest");
        Map<RealizationType, Integer> priorities = Maps.newHashMap();
        priorities.put(RealizationType.HYBRID, 0);
        priorities.put(RealizationType.CUBE, 0);
        priorities.put(RealizationType.INVERTED_INDEX, 0);
        Candidate.setPriorities(priorities);

        joinType = "left";

        setupAll();

        RemoveBlackoutRealizationsRule.blackouts.add("CUBE[name=test_kylin_cube_with_view_left_join_empty]");
        RemoveBlackoutRealizationsRule.blackouts.add("CUBE[name=test_kylin_cube_with_view_inner_join_empty]");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        printInfo("tearDown in ITKylinQueryTest");
        Candidate.restorePriorities();
        clean();
    }

    protected static void setupAll() throws Exception {
        //setup env
        HBaseMetadataTestCase.staticCreateTestMetadata();
        config = KylinConfig.getInstanceFromEnv();

        //setup cube conn
        File olapTmp = OLAPSchemaFactory.createTempOLAPJson(ProjectInstance.DEFAULT_PROJECT_NAME, config);
        Properties props = new Properties();
        props.setProperty(OLAPQuery.PROP_SCAN_THRESHOLD, "10001");
        cubeConnection = DriverManager.getConnection("jdbc:calcite:model=" + olapTmp.getAbsolutePath(), props);

        //setup h2
        h2Connection = DriverManager.getConnection("jdbc:h2:mem:db" + (h2InstanceCount++), "sa", "");
        // Load H2 Tables (inner join)
        H2Database h2DB = new H2Database(h2Connection, config);
        h2DB.loadAllTables();

    }

    protected static void clean() {
        if (cubeConnection != null)
            closeConnection(cubeConnection);
        if (h2Connection != null)
            closeConnection(h2Connection);

        ObserverEnabler.forceCoprocessorUnset();
        HBaseMetadataTestCase.staticCleanupTestMetadata();
        RemoveBlackoutRealizationsRule.blackouts.clear();

    }

    protected String getQueryFolderPrefix() {
        return "";
    }

    @Test
    public void testTimeoutQuery() throws Exception {
        if (HBaseStorage.overwriteStorageQuery != null) {
            //v1 engine does not suit
            return;
        }

        thrown.expect(SQLException.class);

        //should not break at table duplicate check, should fail at model duplicate check
        thrown.expectCause(new BaseMatcher<Throwable>() {
            @Override
            public boolean matches(Object item) {
                if (item instanceof GTScanSelfTerminatedException) {
                    return true;
                }
                return false;
            }

            @Override
            public void describeTo(Description description) {
            }
        });

        try {

            Map<String, String> toggles = Maps.newHashMap();
            toggles.put(BackdoorToggles.DEBUG_TOGGLE_COPROCESSOR_BEHAVIOR, StorageSideBehavior.SCAN_FILTER_AGGR_CHECKMEM_WITHDELAY.toString());//delay 10ms for every scan
            BackdoorToggles.setToggles(toggles);

            KylinConfig.getInstanceFromEnv().setProperty("kylin.query.cube.visit.timeout.times", "0.03");//set timeout to 9s

            //these two cubes has RAW measure, will disturb limit push down
            RemoveBlackoutRealizationsRule.blackouts.add("CUBE[name=test_kylin_cube_without_slr_left_join_empty]");
            RemoveBlackoutRealizationsRule.blackouts.add("CUBE[name=test_kylin_cube_without_slr_inner_join_empty]");

            execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_timeout", null, true);
        } finally {

            //these two cubes has RAW measure, will disturb limit push down
            RemoveBlackoutRealizationsRule.blackouts.remove("CUBE[name=test_kylin_cube_without_slr_left_join_empty]");
            RemoveBlackoutRealizationsRule.blackouts.remove("CUBE[name=test_kylin_cube_without_slr_inner_join_empty]");

            KylinConfig.getInstanceFromEnv().setProperty("kylin.query.cube.visit.timeout.times", "1");//set timeout to 9s 
            BackdoorToggles.cleanToggles();
        }
    }

    //don't try to ignore this test, try to clean your "temp" folder
    @Test
    public void testTempQuery() throws Exception {
        PRINT_RESULT = true;
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/temp", null, true);
        PRINT_RESULT = false;
    }

    @Ignore
    @Test
    public void testSingleRunQuery() throws Exception {

        String queryFileName = getQueryFolderPrefix() + "src/test/resources/query/sql/query01.sql";

        File sqlFile = new File(queryFileName);
        if (sqlFile.exists()) {
            runSQL(sqlFile, true, true);
            runSQL(sqlFile, true, false);
        }
    }

    @Ignore
    @Test
    public void testSingleExecuteQuery() throws Exception {

        String queryFileName = getQueryFolderPrefix() + "src/test/resources/query/sql/query01.sql";

        File sqlFile = new File(queryFileName);
        String sql = getTextFromFile(sqlFile);
        IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);

        executeQuery(kylinConn, queryFileName, sql, true);
    }

    @Ignore
    @Test
    public void testTableauProbing() throws Exception {
        batchExecuteQuery(getQueryFolderPrefix() + "src/test/resources/query/tableau_probing");
    }

    @Test
    public void testCommonQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql", null, true);
    }

    @Test
    public void testLikeQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_like", null, true);
    }

    @Test
    public void testVerifyQuery() throws Exception {
        verifyResultRowCount(getQueryFolderPrefix() + "src/test/resources/query/sql_verifyCount");
    }

    @Test
    public void testOrderByQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_orderby", null, true);
        // FIXME
        // as of optiq 0.8, we lost metadata type with "order by" clause, e.g. sql_orderby/query01.sql
        // thus, temporarily the "order by" clause was cross out, and the needSort is set to true
        // execAndCompQuery("src/test/resources/query/sql_orderby", null, false);
    }

    @Test
    public void testLookupQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_lookup", null, true);
    }

    @Test
    public void testCachedQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_cache", null, true);
    }

    @Test
    public void testDerivedColumnQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_derived", null, true);
    }

    @Test
    public void testDistinctCountQuery() throws Exception {
        if ("left".equalsIgnoreCase(joinType)) {
            batchExecuteQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_distinct");
        }
    }

    @Test
    public void testTopNQuery() throws Exception {
        if ("left".equalsIgnoreCase(joinType)) {
            this.execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_topn", null, true);
        }
    }

    @Test
    public void testPreciselyDistinctCountQuery() throws Exception {
        if ("left".equalsIgnoreCase(joinType)) {
            execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_distinct_precisely", null, true);
        }
    }

    @Test
    public void testDimDistinctCountQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_distinct_dim", null, true);
    }

    @Test
    public void testStreamingTableQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_streaming", null, true);
    }

    @Test
    public void testTableauQuery() throws Exception {
        execAndCompResultSize(getQueryFolderPrefix() + "src/test/resources/query/sql_tableau", null, true);
    }

    @Test
    public void testSubQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_subquery", null, true);
    }

    @Test
    public void testCaseWhen() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_casewhen", null, true);
    }

    @Ignore
    @Test
    public void testHiveQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_hive", null, true);
    }

    @Test
    public void testH2Query() throws Exception {
        this.execQueryUsingH2(getQueryFolderPrefix() + "src/test/resources/query/sql_orderby", false);
    }

    @Test
    public void testInvalidQuery() throws Exception {

        printInfo("-------------------- Test Invalid Query --------------------");
        String queryFolder = getQueryFolderPrefix() + "src/test/resources/query/sql_invalid";
        List<File> sqlFiles = getFilesFromFolder(new File(queryFolder), ".sql");
        for (File sqlFile : sqlFiles) {
            String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
            printInfo("Testing Query " + queryName);
            String sql = getTextFromFile(sqlFile);
            IDatabaseConnection cubeConn = new DatabaseConnection(cubeConnection);
            try {
                cubeConn.createQueryTable(queryName, sql);
            } catch (Throwable t) {
                continue;
            } finally {
                cubeConn.close();
            }
            throw new IllegalStateException(queryName + " should be error!");
        }
    }

    @Test
    public void testDynamicQuery() throws Exception {
        execAndCompDynamicQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_dynamic", null, true);
    }

    @Ignore("simple query will be supported by ii")
    @Test
    public void testLimitEnabled() throws Exception {
        runSqlFile(getQueryFolderPrefix() + "src/test/resources/query/sql_optimize/enable-limit01.sql");
        assertLimitWasEnabled();
    }

    @Test
    public void testLimitCorrectness() throws Exception {
        if (HBaseStorage.overwriteStorageQuery == null) {//v1 query engine will not work
            execLimitAndValidate(getQueryFolderPrefix() + "src/test/resources/query/sql");
        }
    }

    @Test
    public void testRawQuery() throws Exception {
        this.execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_raw", null, true);
    }

    @Test
    public void testGroupingQuery() throws Exception {
        // cannot compare coz H2 does not support grouping set yet..
        this.batchExecuteQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_grouping");
    }

    @Test
    public void testWindowQuery() throws Exception {
        // cannot compare coz H2 does not support window function yet..
        this.batchExecuteQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_window");
    }

    private void assertLimitWasEnabled() {
        OLAPContext context = getFirstOLAPContext();
        assertTrue(context.storageContext.isLimitEnabled());
    }

    private OLAPContext getFirstOLAPContext() {
        return OLAPContext.getThreadLocalContexts().iterator().next();
    }

}

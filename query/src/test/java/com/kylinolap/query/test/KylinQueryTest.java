/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.query.test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.AbstractKylinTestCase;
import com.kylinolap.common.util.HBaseMetadataTestCase;
import com.kylinolap.common.util.HBaseMiniclusterMetadataTestCase;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.metadata.project.ProjectInstance;
import com.kylinolap.query.enumerator.OLAPQuery;
import com.kylinolap.query.relnode.OLAPContext;
import com.kylinolap.query.schema.OLAPSchemaFactory;
import com.kylinolap.storage.hbase.coprocessor.observer.ObserverEnabler;

@Ignore
public class KylinQueryTest extends KylinTestBase {

    @BeforeClass
    public static void setUp() throws Exception {
        printInfo("setUp in KylinQueryTest");

        joinType = "left";
        setupAll();
        preferCubeOf(joinType);
    }

    protected static void setupAll() throws SQLException, IOException, ClassNotFoundException, InterruptedException {
        setUpEnv();
        setUpCubeConn();
        setUpH2Conn();
    }

    private static void setUpEnv() throws IOException, ClassNotFoundException, InterruptedException {
        HBaseMetadataTestCase.staticCreateTestMetadata(AbstractKylinTestCase.MINICLUSTER_TEST_DATA);
        HBaseMiniclusterMetadataTestCase.startupMinicluster();
        config = KylinConfig.getInstanceFromEnv();
    }

    private static void setUpCubeConn() throws SQLException {
        // Cube Connection
        File olapTmp = OLAPSchemaFactory.createTempOLAPJson(ProjectInstance.DEFAULT_PROJECT_NAME, config);
        Properties props = new Properties();
        props.setProperty(OLAPQuery.PROP_SCAN_THRESHOLD, "10000");
        cubeConnection = DriverManager.getConnection("jdbc:calcite:model=" + olapTmp.getAbsolutePath(), props);
    }

    private static void setUpH2Conn() throws SQLException {
        // H2 Connection
        h2Connection = DriverManager.getConnection("jdbc:h2:mem:db" + (h2InstanceCount++), "sa", "");
        // Load H2 Tables (inner join)
        H2Database h2DB = new H2Database(h2Connection, config);
        h2DB.loadAllTables(joinType);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        printInfo("tearDown");
        printInfo("Closing connection...");
        clean();
        HBaseMiniclusterMetadataTestCase.shutdownMiniCluster();
    }

    protected static void clean() {
        if (cubeConnection != null)
            closeConnection(cubeConnection);
        if (h2Connection != null)
            closeConnection(h2Connection);

        ObserverEnabler.forceCoprocessorUnset();
        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    protected static void preferCubeOf(String joinType) {

        CubeManager cubeManager = CubeManager.getInstance(config);

        boolean cubesBuiltInBatch = cubeManager.getCube("test_kylin_cube_with_slr_empty") != null && cubeManager.getCube("test_kylin_cube_without_slr_empty") != null && cubeManager.getCube("test_kylin_cube_with_slr_left_join_empty") != null && cubeManager.getCube("test_kylin_cube_without_slr_left_join_empty") != null;

        if (!cubesBuiltInBatch) {
            printInfo("Four empty cubes built in BuildCubeWithEngineTest is not complete, preferCubeOf being ignored");
            return;
        }

        if (joinType.equals("inner")) {
            cubeManager.getCube("test_kylin_cube_with_slr_empty").setCost(20);
            cubeManager.getCube("test_kylin_cube_without_slr_empty").setCost(10);
            cubeManager.getCube("test_kylin_cube_with_slr_left_join_empty").setCost(100);
            cubeManager.getCube("test_kylin_cube_without_slr_left_join_empty").setCost(90);
        } else if (joinType.equals("left") || joinType.equals("default")) {
            cubeManager.getCube("test_kylin_cube_with_slr_empty").setCost(100);
            cubeManager.getCube("test_kylin_cube_without_slr_empty").setCost(90);
            cubeManager.getCube("test_kylin_cube_with_slr_left_join_empty").setCost(20);
            cubeManager.getCube("test_kylin_cube_without_slr_left_join_empty").setCost(10);
        }
    }

    // for debug purpose
    @Ignore
    @Test
    public void testTempQuery() throws Exception {
       execAndCompQuery("src/test/resources/query/temp", null, true);
    }

    @Test
    public void testSingleRunQuery() throws Exception {

        String queryFileName = "src/test/resources/query/sql_orderby/query01.sql";

        File sqlFile = new File(queryFileName);
        runSQL(sqlFile, true, true);
        runSQL(sqlFile, true, false);
    }

    @Test
    public void testSingleExecuteQuery() throws Exception {

        String queryFileName = "src/test/resources/query/sql/query39.sql";

        File sqlFile = new File(queryFileName);
        String sql = getTextFromFile(sqlFile);
        IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);

        executeQuery(kylinConn, queryFileName, sql, true);
    }

    @Ignore
    @Test
    public void testTableauProbing() throws Exception {
        batchExecuteQuery("src/test/resources/query/tableau_probing");
    }

    @Test
    public void testCommonQuery() throws Exception {
        execAndCompQuery("src/test/resources/query/sql", null, true);
    }

    @Test
    public void testSimpleQuery() throws Exception {
        verifyResultRowCount("src/test/resources/query/sql_verifyCount");
    }

    @Test
    public void testOrderByQuery() throws Exception {
        execAndCompQuery("src/test/resources/query/sql_orderby", null, true);
        // FIXME
        // as of optiq 0.8, we lost metadata type with "order by" clause, e.g. sql_orderby/query01.sql
        // thus, temporarily the "order by" clause was cross out, and the needSort is set to true
        // execAndCompQuery("src/test/resources/query/sql_orderby", null, false);
    }

    @Test
    public void testLookupQuery() throws Exception {
        execAndCompQuery("src/test/resources/query/sql_lookup", null, true);
    }

    @Test
    public void testDerivedColumnQuery() throws Exception {
        execAndCompQuery("src/test/resources/query/sql_derived", null, true);
    }

    @Test
    public void testDistinctCountQuery() throws Exception {
        batchExecuteQuery("src/test/resources/query/sql_distinct");
    }

    @Test
    public void testTableauQuery() throws Exception {
        batchExecuteQuery("src/test/resources/query/sql_tableau");
    }

    @Test
    public void testSubQuery() throws Exception {
        execAndCompQuery("src/test/resources/query/sql_subquery", null, true);
    }

    @Test
    public void testCaseWhen() throws Exception {
        execAndCompQuery("src/test/resources/query/sql_casewhen", null, true);
    }

    @Ignore
    @Test
    public void testHiveQuery() throws Exception {
        execAndCompQuery("src/test/resources/query/sql_hive", null, true);
    }

    @Test
    public void testH2Query() throws Exception {
        this.execQueryUsingH2("src/test/resources/query/sql_orderby", false);
    }

    @Test
    public void testInvalidQuery() throws Exception {

        printInfo("-------------------- Test Invalid Query --------------------");
        String queryFolder = "src/test/resources/query/sql_invalid";
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
        execAndCompDynamicQuery("src/test/resources/query/sql_dynamic", null, true);
    }

    @Test
    public void testLimitEnabled() throws Exception {
        runSqlFile("src/test/resources/query/sql_optimize/enable-limit01.sql");
        assertLimitWasEnabled();
    }

    private void assertLimitWasEnabled() {
        OLAPContext context = getFirstOLAPContext();
        assertTrue(context.storageContext.isLimitEnabled());
    }

    private OLAPContext getFirstOLAPContext() {
        return OLAPContext.getThreadLocalContexts().iterator().next();
    }

}

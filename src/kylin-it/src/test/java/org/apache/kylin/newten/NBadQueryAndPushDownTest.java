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

package org.apache.kylin.newten;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.List;

import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.KylinTestBase;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparderEnv;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class NBadQueryAndPushDownTest extends NLocalWithSparkSessionTest {
    private static final String PUSHDOWN_RUNNER_KEY = "kylin.query.pushdown.runner-class-name";
    private static final String PUSHDOWN_ENABLED = "kylin.query.pushdown-enabled";
    private static final String PROJECT_NAME = "bad_query_test";
    private static final String DEFAULT_PROJECT_NAME = "default";

    @Override
    public String getProject() {
        return PROJECT_NAME;
    }

    @Test
    public void testTableNotFoundInDatabase() throws Exception {
        //from tpch database
        final String sql = "select * from lineitem where l_orderkey = o.o_orderkey and l_commitdate < l_receiptdate";
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl");
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "true");
        try {
            ExecAndComp.queryModelWithoutCompute(getProject(), sql);
        } catch (Exception sqlException) {
            Assert.assertTrue(sqlException instanceof SQLException);
            Assert.assertTrue(ExceptionUtils.getRootCause(sqlException) instanceof SqlValidatorException);
        }
    }

    @Test
    public void testPushdownCCWithFn() throws Exception {
        final String sql = "select LSTG_FORMAT_NAME,sum(NEST4) from TEST_KYLIN_FACT where ( not { fn convert( \"LSTG_FORMAT_NAME\", SQL_WVARCHAR ) } = 'ABIN' or \"LSTG_FORMAT_NAME\" is null) group by LSTG_FORMAT_NAME limit 1";
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl");
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "true");

        // success
        pushDownSql(DEFAULT_PROJECT_NAME, sql, 10, 0, null, true);

        // failed for wrong pushdown converter order
        overwriteSystemProp("kylin.query.pushdown.converter-class-names",
                "org.apache.kylin.query.util.SparkSQLFunctionConverter,org.apache.kylin.query.util.PowerBIConverter,org.apache.kylin.query.util.RestoreFromComputedColumn,org.apache.kylin.query.security.RowFilter,org.apache.kylin.query.security.HackSelectStarWithColumnACL");
        try {
            pushDownSql(DEFAULT_PROJECT_NAME, sql, 10, 0, null, true);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof AnalysisException);
            Assert.assertTrue(e.getMessage().contains("Column 'NEST4' does not exist"));
        }
    }

    @Test
    public void testPushDownToNonExistentDB() throws Exception {
        //from tpch database
        try {
            final String sql = "select * from lineitem where l_orderkey = o.o_orderkey and l_commitdate < l_receiptdate";
            KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                    "org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl");
            KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "true");
            pushDownSql(getProject(), sql, 0, 0,
                    new SQLException(new NoRealizationFoundException("testPushDownToNonExistentDB")), true);
        } catch (Exception e) {
            Assert.assertTrue(ExceptionUtils.getRootCause(e) instanceof AnalysisException);
            Assert.assertTrue(ExceptionUtils.getRootCauseMessage(e).contains("Table or view not found: LINEITEM"));
        }
    }

    @Test
    public void testPushDownForFileNotExist() throws Exception {
        final String sql = "select max(price) from test_kylin_fact";
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl");
        try {
            ExecAndComp.queryModelWithoutCompute(getProject(), sql);
        } catch (Exception sqlException) {
            if (sqlException instanceof SQLException) {
                Assert.assertTrue(ExceptionUtils.getRootCauseMessage(sqlException).contains("Path does not exist"));
                pushDownSql(getProject(), sql, 0, 0, (SQLException) sqlException);
            }
        }
    }

    @Test
    public void testPushDownWithSemicolonQuery() throws Exception {
        final String sql = "select 1 from test_kylin_fact;";
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl");
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "true");
        pushDownSql(getProject(), sql, 10, 0,
                new SQLException(new NoRealizationFoundException("test for semicolon query push down")), true);
        try {
            pushDownSql(getProject(), sql, 10, 1,
                    new SQLException(new NoRealizationFoundException("test for semicolon query push down")), true);
        } catch (Exception sqlException) {
            Assert.assertTrue(ExceptionUtils.getRootCauseMessage(sqlException).contains("input 'OFFSET'"));
        }
    }

    @Test
    public void testPushDownNonEquiSql() throws Exception {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.pushdown.converter-class-names",
                "org.apache.kylin.query.util.RestoreFromComputedColumn,org.apache.kylin.query.util.SparkSQLFunctionConverter");
        File sqlFile = new File("src/test/resources/query/sql_pushdown/query11.sql");
        String sql = new String(Files.readAllBytes(sqlFile.toPath()), StandardCharsets.UTF_8);
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "false");
        try {
            ExecAndComp.queryModelWithoutCompute(DEFAULT_PROJECT_NAME, sql);
        } catch (Exception e) {
            KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                    "org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl");
            KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "true");
            pushDownSql(DEFAULT_PROJECT_NAME, sql, 0, 0, (SQLException) e);
        }
    }

    @Test
    public void testPushDownUdf() throws Exception {
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl");
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "true");
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.pushdown.converter-class-names",
                "org.apache.kylin.query.util.RestoreFromComputedColumn,org.apache.kylin.query.util.SparkSQLFunctionConverter");

        String prjName = "tdvt";
        // timstampDiff
        String sql = "SELECT {fn TIMESTAMPDIFF(SQL_TSI_DAY,{d '1900-01-01'},\"CALCS\".\"DATE0\")} AS \"TEMP_Test__2048215813__0_\"\n"
                + "FROM \"CALCS\" \"CALCS\"\n"
                + "GROUP BY {fn TIMESTAMPDIFF(SQL_TSI_DAY,{d '1900-01-01'},\"CALCS\".\"DATE0\")}";
        val result = pushDownSql(prjName, sql, 0, 0,
                new SQLException(new NoRealizationFoundException("test for  query push down")), true);
        Assert.assertNotNull(result);

        //timestampAdd
        sql = "  SELECT {fn TIMESTAMPADD(SQL_TSI_DAY,1,\"CALCS\".\"DATE2\")} AS \"TEMP_Test__3825428522__0_\"\n"
                + "FROM \"CALCS\" \"CALCS\"\n" + "GROUP BY {fn TIMESTAMPADD(SQL_TSI_DAY,1,\"CALCS\".\"DATE2\")}";
        val result1 = pushDownSql(prjName, sql, 0, 0,
                new SQLException(new NoRealizationFoundException("test for  query push down")), true);
        Assert.assertNotNull(result1);

        // TRUNCATE
        sql = "SELECT {fn CONVERT({fn TRUNCATE(\"CALCS\".\"NUM4\",0)}, SQL_BIGINT)} AS \"TEMP_Test__4269159351__0_\"\n"
                + "FROM \"CALCS\" \"CALCS\"\n"
                + "GROUP BY {fn CONVERT({fn TRUNCATE(\"CALCS\".\"NUM4\",0)}, SQL_BIGINT)}";
        val result2 = pushDownSql(prjName, sql, 0, 0,
                new SQLException(new NoRealizationFoundException("test for  query push down")), true);
        Assert.assertNotNull(result2);
    }

    @Test
    public void testPushDownForced() throws Exception {

        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl");
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "true");
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.pushdown.converter-class-names",
                "org.apache.kylin.query.util.RestoreFromComputedColumn,org.apache.kylin.query.util.SparkSQLFunctionConverter");

        String prjName = "tdvt";
        // timstampDiff
        String sql = "SELECT {fn TIMESTAMPDIFF(SQL_TSI_DAY,{d '1900-01-01'},\"CALCS\".\"DATE0\")} AS \"TEMP_Test__2048215813__0_\"\n"
                + "FROM \"CALCS\" \"CALCS\"\n"
                + "GROUP BY {fn TIMESTAMPDIFF(SQL_TSI_DAY,{d '1900-01-01'},\"CALCS\".\"DATE0\")}";
        val resultForced = pushDownSql(prjName, sql, 0, 0, null, true);
        Assert.assertNotNull(resultForced);

        //test for error when  execption is null and is not forced
        try {
            pushDownSql(prjName, sql, 0, 0, null, false);
            Assert.fail();
        } catch (Exception e) {
            Throwable rootCause = Throwables.getRootCause(e);
            Assert.assertTrue(rootCause instanceof IllegalArgumentException);
        }

        //test for error when  pushdown turn off, and force to push down
        try {
            KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "false");
            Assert.assertFalse(KylinConfig.getInstanceFromEnv().isPushDownEnabled());
            pushDownSql(prjName, sql, 0, 0, null, true);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(((KylinException) e).getErrorCode(),
                    QueryErrorCode.INVALID_PARAMETER_PUSH_DOWN.toErrorCode());
            Assert.assertEquals(MsgPicker.getMsg().getDisablePushDownPrompt(), Throwables.getRootCause(e).getMessage());
        } finally {
            KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "true");
        }
    }

    private Pair<List<List<String>>, List<SelectedColumnMeta>> pushDownSql(String prjName, String sql, int limit,
            int offset, SQLException sqlException) throws Exception {
        return pushDownSql(prjName, sql, limit, offset, sqlException, false);
    }

    private Pair<List<List<String>>, List<SelectedColumnMeta>> pushDownSql(String prjName, String sql, int limit,
            int offset, SQLException sqlException, boolean isForced) throws Exception {
        populateSSWithCSVData(KylinConfig.getInstanceFromEnv(), prjName, SparderEnv.getSparkSession());
        String pushdownSql = ExecAndComp.removeDataBaseInSql(sql);
        String massagedSql = QueryUtil.appendLimitOffset(prjName, pushdownSql, limit, offset);
        QueryParams queryParams = new QueryParams(prjName, massagedSql, "DEFAULT", BackdoorToggles.getPrepareOnly(),
                sqlException, isForced);
        queryParams.setSelect(true);
        queryParams.setLimit(limit);
        queryParams.setOffset(offset);
        Pair<List<List<String>>, List<SelectedColumnMeta>> result = KylinTestBase.tryPushDownQuery(queryParams);
        if (result == null) {
            throw sqlException;
        }
        return result;
    }
}

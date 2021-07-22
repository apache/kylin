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
package org.apache.kylin.engine.spark2;

import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.spark.sql.KylinSparkEnv;
import org.apache.spark.sql.SparderContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Ignore;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.List;

public class NBadQueryAndPushDownTest extends LocalWithSparkSessionTest {
    private static final String PUSHDOWN_RUNNER_KEY = "kylin.query.pushdown.runner-class-name";
    private final static String DEFAULT_PROJECT_NAME = "default";

    @Override
    public void setup() throws SchedulerException {
        super.setup();
        SparderContext.setSparkSession(KylinSparkEnv.getSparkSession());
    }

    @Override
    public String getProject() {
        return DEFAULT_PROJECT_NAME;
    }

    @Override
    public void after() {
        super.after();
    }

    @Test
    public void testTableNotFoundInDatabase() {
        //from tpch database
        final String sql = "select * from lineitem where l_orderkey = o.o_orderkey and l_commitdate < l_receiptdate";
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl");
        try {
            NExecAndComp.queryCubeAndSkipCompute(getProject(), sql);
        } catch (Exception sqlException) {
            Assert.assertTrue(sqlException instanceof SQLException);
            Assert.assertTrue(ExceptionUtils.getRootCause(sqlException) instanceof SqlValidatorException);
        }
    }

    @Ignore
    public void testPushDownToNonExistentDB() {
        //from tpch database
        try {
            final String sql = "select * from lineitem where l_orderkey = o.o_orderkey and l_commitdate < l_receiptdate";
            KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                    "org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl");
            pushDownSql(getProject(), sql, 0, 0,
                    new SQLException(new NoRealizationFoundException("testPushDownToNonExistentDB")));
        } catch (Exception e) {
            Assert.assertTrue(ExceptionUtils.getRootCauseMessage(e)
                    .contains("Table or view not found: lineitem"));
        }
    }

    @Test
    public void testPushDownForFileNotExist() throws Exception {
        final String sql = "select max(ITEM_COUNT) from test_kylin_fact";
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl");
        try {
            NExecAndComp.queryCubeAndSkipCompute(getProject(), sql);
        } catch (Exception sqlException) {
            if (sqlException instanceof SQLException) {
                Assert.assertTrue(ExceptionUtils.getRootCauseMessage(sqlException).contains("NoRealizationFoundException"));
                pushDownSql(getProject(), sql, 0, 0, (SQLException) sqlException);
            }
        }
    }

    @Test
    public void testPushDownNonEquiSql() throws Exception {
        File sqlFile = new File("src/test/resources/query/sql_pushdown/query11.sql");
        String sql = new String(Files.readAllBytes(sqlFile.toPath()), StandardCharsets.UTF_8);
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl");
        try {
            NExecAndComp.queryCubeAndSkipCompute(DEFAULT_PROJECT_NAME, sql);
        } catch (Exception e) {
            if (e instanceof SQLException)
                pushDownSql(getProject(), sql, 0, 0, (SQLException) e);
        }
    }

    private Pair<List<List<String>>, List<SelectedColumnMeta>> pushDownSql(String prjName, String sql, int limit,
                                                                           int offset, SQLException sqlException)
            throws Exception {
        populateSSWithCSVData(KylinConfig.getInstanceFromEnv(), prjName, KylinSparkEnv.getSparkSession());
        String pushdownSql = NExecAndComp.removeDataBaseInSql(sql);
        String massagedSql = QueryUtil.appendLimitOffsetToSql(pushdownSql, limit, offset);
        Pair<List<List<String>>, List<SelectedColumnMeta>> result = PushDownUtil.tryPushDownSelectQuery(prjName,
                massagedSql, "DEFAULT", sqlException, BackdoorToggles.getPrepareOnly());
        if (result == null) {
            throw sqlException;
        }
        return result;
    }
}

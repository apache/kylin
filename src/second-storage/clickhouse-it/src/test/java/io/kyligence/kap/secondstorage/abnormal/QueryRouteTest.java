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
package io.kyligence.kap.secondstorage.abnormal;

import static io.kyligence.kap.clickhouse.ClickHouseConstants.CONFIG_CLICKHOUSE_QUERY_CATALOG;
import static io.kyligence.kap.newten.clickhouse.ClickHouseUtils.columnMapping;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasource.FilePruner;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.google.common.collect.ImmutableMap;

import io.kyligence.kap.newten.clickhouse.ClickHouseUtils;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.test.ClickHouseClassRule;
import io.kyligence.kap.secondstorage.test.EnableClickHouseJob;
import io.kyligence.kap.secondstorage.test.EnableTestUser;
import io.kyligence.kap.secondstorage.test.SharedSparkSession;

/**
 * {@link QueryRouteTest} simulates the case where KE(resolving clickhouse table schema on spark driver) or worker
 * (executing query on spark executor) can not access ClickHouse.In such case, we should use table index to answer query.
 * <p/>
 *  @see <a href="https://olapio.atlassian.net/browse/KE-28035">KE-28035</a> for details.
 */
public class QueryRouteTest {

    static private final String testSQL = "select sum(PRICE) from TEST_KYLIN_FACT group by PRICE";
    static private final String cubeName = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";
    static private final String project = "table_index";
    static private final int clickhouseNumber = 2;

    @ClassRule
    public static SharedSparkSession sharedSpark = new SharedSparkSession(
            ImmutableMap.of("spark.sql.extensions", "io.kyligence.kap.query.SQLPushDownExtensions"));
    @ClassRule
    public static ClickHouseClassRule clickHouseClassRule = new ClickHouseClassRule(clickhouseNumber);
    public EnableTestUser enableTestUser = new EnableTestUser();
    public EnableClickHouseJob test = new EnableClickHouseJob(clickHouseClassRule.getClickhouse(), 1, project,
            Collections.singletonList(cubeName), "src/test/resources/ut_meta");
    @Rule
    public TestRule rule = RuleChain.outerRule(enableTestUser).around(test);
    private final SparkSession sparkSession = sharedSpark.getSpark();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Ignore("It is conflict with EnableClickHouseJob, see https://olapio.atlassian.net/browse/KE-29578 for more info.")
    @Test
    public void testWhenCannotAccessClickHouse() throws Exception {
        final String queryCatalog = "testWhenCannotAccessClickHouse";
        try {
            Unsafe.setProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG, queryCatalog);

            //build
            new IndexDataConstructor(project).buildDataflow(cubeName);
            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            Assert.assertEquals(1, SecondStorageUtil.setSecondStorageSizeInfo(modelManager.listAllModels()).size());

            // check
            test.checkHttpServer();
            test.overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");

            JdbcDatabaseContainer<?> clickhouse1 = clickHouseClassRule.getClickhouse(1);
            sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog,
                    "org.apache.spark.sql.execution.datasources.jdbc.v2.SecondStorageCatalog");
            sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog + ".url",
                    clickhouse1.getJdbcUrl());
            sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog + ".driver",
                    clickhouse1.getDriverClassName());
            sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog + ".pushDownAggregate",
                    "true");
            sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog + ".numPartitions",
                    String.valueOf(clickhouseNumber));

            //check we get the correct plan
            Dataset<Row> groupPlan = ExecAndComp.queryModelWithoutCompute(project, testSQL);
            JDBCScan jdbcScan = ClickHouseUtils.findJDBCScan(groupPlan.queryExecution().optimizedPlan());
            Assert.assertEquals(clickhouseNumber, jdbcScan.relation().parts().length);
            ClickHouseUtils.checkAggregateRemoved(groupPlan);
            String[] expectedPlanFragment = new String[] {
                    "PushedAggregates: [SUM(" + columnMapping.get("PRICE") + ")], ", "PushedFilters: [], ",
                    "PushedGroupByColumns: [" + columnMapping.get("PRICE") + "], " };
            ClickHouseUtils.checkPushedInfo(groupPlan, expectedPlanFragment);

            //check result
            NLocalWithSparkSessionTest.populateSSWithCSVData(test.getTestConfig(), project, sparkSession);
            List<Pair<String, String>> query = new ArrayList<>();
            query.add(Pair.newPair("query_table_index1", testSQL));
            ExecAndComp.execAndCompare(query, project, ExecAndComp.CompareLevel.SAME, "left");

            //close one of clickhouse
            JdbcDatabaseContainer<?> clickhouse0 = clickHouseClassRule.getClickhouse(0);
            clickhouse0.close();

            // Now rerun it, using
            Dataset<Row> groupPlanWithTableIndex = ExecAndComp.queryModel(project, testSQL);
            FilePruner filePruner = ClickHouseUtils
                    .findFilePruner(groupPlanWithTableIndex.queryExecution().optimizedPlan());
            Assert.assertEquals(cubeName, filePruner.options().get("dataflowId").get());

            /**
             *  Now we delete working dir to make worker fail again which depends on cache of FilePruner to workaround
             *  Spark Planning. See {@link FilePruner#getFileStatues}.
             *  <p/>
             *  Please note, it is ok that {@link QueryContext#isForceTableIndex()} currently return true, since we only
             *  expect {@link SQLException} is thrown, and later {@link org.apache.kylin.rest.service.QueryService} will
             * go through push-down.
             */
            File file = new File(new URI(filePruner.workingDir()));
            Assert.assertTrue(file.exists());
            Assert.assertTrue(file.isDirectory());
            FileUtils.deleteDirectory(file);
            // re run again, expect exception
            thrown.expect(SQLException.class);
            thrown.expectCause(IsInstanceOf.instanceOf(SparkException.class));
            thrown.expectCause(new BaseMatcher<Throwable>() {

                @Override
                public void describeTo(Description description) {

                }

                @Override
                public boolean matches(Object item) {
                    if (item instanceof SparkException) {
                        return ((SparkException) item).getCause() instanceof FileNotFoundException;
                    }
                    return false;
                }
            });

            ExecAndComp.queryModel(project, testSQL);
        } finally {
            Unsafe.clearProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG);
        }
    }

    @Test
    public void testWhenCatalogThrowException() throws Exception {
        final String queryCatalog = "testWhenCatalogReturnNone";
        try {
            Unsafe.setProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG, queryCatalog);

            //build
            new IndexDataConstructor(project).buildDataflow(cubeName);
            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            Assert.assertEquals(3, SecondStorageUtil.setSecondStorageSizeInfo(modelManager.listAllModels()).size());

            // check
            test.checkHttpServer();
            test.overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");

            JdbcDatabaseContainer<?> clickhouse = clickHouseClassRule.getClickhouse(0);
            sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog,
                    "io.kyligence.kap.secondstorage.abnormal.AlwaysSQLExceptionCatalog");
            sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog + ".url",
                    clickhouse.getJdbcUrl());
            sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog + ".driver",
                    clickhouse.getDriverClassName());

            Dataset<Row> groupPlan = ExecAndComp.queryModelWithoutCompute(project, testSQL);
            FilePruner filePruner = ClickHouseUtils.findFilePruner(groupPlan.queryExecution().optimizedPlan());
            Assert.assertEquals(cubeName, filePruner.options().get("dataflowId").get());
        } finally {
            Unsafe.clearProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG);
        }
    }
}

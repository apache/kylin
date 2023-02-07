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
package io.kyligence.kap.secondstorage.tdvt;

import static io.kyligence.kap.clickhouse.ClickHouseConstants.CONFIG_CLICKHOUSE_QUERY_CATALOG;
import static org.apache.kylin.engine.spark.NLocalWithSparkSessionTest.populateSSWithCSVData;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.HiveResult$;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.kyligence.kap.newten.clickhouse.ClickHouseUtils;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.test.ClickHouseClassRule;
import io.kyligence.kap.secondstorage.test.EnableClickHouseJob;
import io.kyligence.kap.secondstorage.test.EnableTestUser;
import io.kyligence.kap.secondstorage.test.SetTimeZone;
import io.kyligence.kap.secondstorage.test.SharedSparkSession;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import scala.math.Ordering;

@RunWith(Parameterized.class)
@Slf4j
public class TDVTHiveTest {

    static private final String project = "tdvt_new";
    static private final String AUTO_MODEL_CALCS_1 = "d4ebc34f-ec70-4e81-830c-0d278fe064aa";
    static private final String AUTO_MODEL_STAPLES_1 = "0dabbdd5-7246-4fdb-b2a9-5398dc4c57f7";
    static private final int clickhouseNumber = 1;
    static private final List<String> modelList = ImmutableList.of(AUTO_MODEL_CALCS_1, AUTO_MODEL_STAPLES_1);
    static private final String queryCatalog = TDVTTest.class.getSimpleName();
    static private ImmutableSet<String> blackList = ImmutableSet.of("untest.sql", "sql038.sql", "sql108.sql",
            "sql618.sql");

    @ClassRule
    public static SharedSparkSession sharedSpark = new SharedSparkSession(
            ImmutableMap.of("spark.sql.extensions", "io.kyligence.kap.query.SQLPushDownExtensions"));

    public static EnableTestUser enableTestUser = new EnableTestUser();
    public static ClickHouseClassRule clickHouse = new ClickHouseClassRule(clickhouseNumber);
    public static EnableClickHouseJob test = new EnableClickHouseJob(clickHouse.getClickhouse(), 1, project, modelList,
            "src/test/resources/ut_meta");
    public static SetTimeZone timeZone = new SetTimeZone("UTC"); // default timezone of clickhouse docker is UTC
    @ClassRule
    public static TestRule rule = RuleChain.outerRule(enableTestUser).around(clickHouse).around(test).around(timeZone);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<String[]> testSQLs() {
        final URL resourceRoot = Objects.requireNonNull(TDVTTest.class.getClassLoader().getResource("tdvt"));
        final File baseResourcePath = new File(resourceRoot.getFile());

        final String inputFilePath = new File(baseResourcePath, "inputs").getAbsolutePath();
        final File[] sql = Objects
                .requireNonNull(new File(inputFilePath).listFiles((dir, name) -> name.endsWith(".sql")));
        final String goldenFilePath = new File(baseResourcePath, "results").getAbsolutePath();

        return Stream.of(sql).map(file -> {
            final String sqlPath = file.getAbsolutePath();
            final String resultFile = file.getAbsolutePath().replace(inputFilePath, goldenFilePath) + ".out";
            final String testCaseName = StringUtils.substringAfter(sqlPath, inputFilePath + File.separator);
            return new String[] { testCaseName, sqlPath, resultFile };
        }).filter(objects -> !blackList.contains(objects[0].toLowerCase(Locale.ROOT))).collect(Collectors.toList());
    }

    @BeforeClass
    public static void beforeClass() throws Exception {

        Unsafe.setProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG, queryCatalog);
        //build
        val constructor = new IndexDataConstructor(project);
        constructor.buildDataflow(AUTO_MODEL_CALCS_1);
        constructor.buildDataflow(AUTO_MODEL_STAPLES_1);
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        Assert.assertEquals(2, SecondStorageUtil.setSecondStorageSizeInfo(modelManager.listAllModels()).size());

        // check
        test.checkHttpServer();
        test.overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");

        JdbcDatabaseContainer<?> ckInstance = clickHouse.getClickhouse(0);
        final SparkSession sparkSession = sharedSpark.getSpark();
        sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog,
                "org.apache.spark.sql.execution.datasources.jdbc.v2.SecondStorageCatalog");
        sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog + ".url",
                ckInstance.getJdbcUrl());
        sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog + ".driver",
                ckInstance.getDriverClassName());
        sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog + ".pushDownAggregate",
                "true");
        sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog + ".pushDownLimit",
                "true");
        sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog + ".numPartitions", "1");

        populateSSWithCSVData(test.getTestConfig(), project, SparderEnv.getSparkSession());
    }

    @AfterClass
    public static void afterClass() {
        Unsafe.clearProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG);
    }

    private final String testName;
    private final String inputSqlPath;
    private final String resultPath;

    public TDVTHiveTest(String testName, String sqlPath, String resultPath) {
        this.testName = testName;
        this.inputSqlPath = sqlPath;
        this.resultPath = resultPath;
    }

    private String readSQL() throws IOException {
        if (inputSqlPath.startsWith("SELECT"))
            return inputSqlPath;
        else
            return FileUtils.readFileToString(new File(inputSqlPath), "UTF-8").trim();
    }

    @Test
    public void testRunSql() throws Exception {
        String sqlStatement = readSQL();
        String resultPush = runWithAggPushDown(sqlStatement);
        String resultTableIndex = runWithHive(sqlStatement);
        log.info("SQL:{}", sqlStatement);
        Assert.assertEquals(resultTableIndex, resultPush);
        Assert.assertTrue(true);
    }

    private String runWithAggPushDown(String sqlStatement) throws Exception {
        QueryContext.current().setForceTableIndex(false);
        Dataset<Row> plan = ExecAndComp.queryModelWithoutCompute(project, sqlStatement);
        JDBCScan jdbcScan = ClickHouseUtils.findJDBCScan(plan.queryExecution().optimizedPlan());
        Assert.assertNotNull(jdbcScan);
        return computeResult(plan);
    }

    private static String computeResult(Dataset<Row> plan) {
        return HiveResult$.MODULE$.hiveResultString(plan.queryExecution().executedPlan())
                .sorted(Ordering.String$.MODULE$).mkString("\n");
    }

    private String runWithHive(String sqlStatement) {
        QueryParams queryParams = new QueryParams(project, sqlStatement, "default", false);
        queryParams.setKylinConfig(NProjectManager.getProjectConfig(project));
        String afterConvert = PushDownUtil.massagePushDownSql(queryParams);
        // Table schema comes from csv and DATABASE.TABLE is not supported.
        String sqlForSpark = ExecAndComp.removeDataBaseInSql(afterConvert);
        Dataset<Row> plan = ExecAndComp.querySparkSql(sqlForSpark);
        return computeResult(plan);
    }
}

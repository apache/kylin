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
import static org.apache.kylin.engine.spark.IndexDataConstructor.firstFailedJobErrorMessage;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.job.SecondStorageJobParamUtil;
import org.apache.kylin.job.common.ExecutableUtil;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.handler.AbstractJobHandler;
import org.apache.kylin.job.handler.SecondStorageSegmentLoadJobHandler;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.HiveResult$;
import org.apache.spark.sql.execution.datasource.FilePruner;
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

import io.kyligence.kap.newten.clickhouse.ClickHouseUtils;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.test.ClickHouseClassRule;
import io.kyligence.kap.secondstorage.test.EnableClickHouseJob;
import io.kyligence.kap.secondstorage.test.EnableTestUser;
import io.kyligence.kap.secondstorage.test.SetTimeZone;
import io.kyligence.kap.secondstorage.test.SharedSparkSession;
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import scala.math.Ordering;

@RunWith(Parameterized.class)
@Slf4j
public class TDVTTest implements JobWaiter {

    static private final String project = "tdvt_new";
    static private final String AUTO_MODEL_CALCS_1 = "d4ebc34f-ec70-4e81-830c-0d278fe064aa";
    static private final String AUTO_MODEL_STAPLES_1 = "0dabbdd5-7246-4fdb-b2a9-5398dc4c57f7";
    static private final int clickhouseNumber = 1;
    static private final List<String> modelList = ImmutableList.of(AUTO_MODEL_CALCS_1, AUTO_MODEL_STAPLES_1);
    static private final String queryCatalog = TDVTTest.class.getSimpleName();

    static private final ImmutableSet<String> whiteSQLList = ImmutableSet.of();
    static private Set<String> blackSQLList = null;

    @ClassRule
    public static SharedSparkSession sharedSpark = new SharedSparkSession(
            ImmutableMap.of("spark.sql.extensions", "org.apache.kylin.query.SQLPushDownExtensions"));

    public static EnableTestUser enableTestUser = new EnableTestUser();
    public static ClickHouseClassRule clickHouse = new ClickHouseClassRule(clickhouseNumber);
    public static EnableClickHouseJob test = new EnableClickHouseJob(clickHouse.getClickhouse(), 1, project, modelList,
            "src/test/resources/ut_meta");
    public static SetTimeZone timeZone = new SetTimeZone("UTC"); // default timezone of clickhouse docker is UTC

    @ClassRule
    public static TestRule rule = RuleChain.outerRule(enableTestUser).around(clickHouse).around(test).around(timeZone);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<String[]> testSQLs() throws IOException {
        final URL resourceRoot = Objects.requireNonNull(TDVTTest.class.getClassLoader().getResource("tdvt"));
        final File baseResourcePath = new File(resourceRoot.getFile());
        final String tdvtIgnoreFilePath = new File(baseResourcePath, "tdvt.ignore").getAbsolutePath();

        try (Stream<String> lines = Files.lines(Paths.get(tdvtIgnoreFilePath), Charset.defaultCharset())) {
            blackSQLList = lines.map(line -> {
                int firsColon = line.indexOf(':');
                String ignoreTestCase = line.substring(0, firsColon).toLowerCase(Locale.ROOT);
                String reason = line.substring(firsColon + 1);
                return ignoreTestCase;
            }).collect(Collectors.toSet());
        }

        final String tdvtFilePath = new File(baseResourcePath, "tdvt").getAbsolutePath();

        try (Stream<String> lines = Files.lines(Paths.get(tdvtFilePath), Charset.defaultCharset())) {
            return lines.map(line -> {
                int firsColon = line.indexOf(':');
                String testCaseName = line.substring(0, firsColon).toLowerCase(Locale.ROOT);
                String sql = line.substring(firsColon + 1);
                return new String[] { testCaseName, sql, null };
            }).filter(objects -> whiteSQLList.isEmpty() || whiteSQLList.contains(objects[0]))
                    .filter(objects -> !blackSQLList.contains(objects[0])).collect(Collectors.toList());
        }
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

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        waitFinish(project,
                triggerClickHouseLoad(project, AUTO_MODEL_CALCS_1, "ADMIN",
                        dataflowManager.getDataflow(AUTO_MODEL_CALCS_1).getSegments().stream().map(NDataSegment::getId)
                                .collect(Collectors.toList())));
        waitFinish(project,
                triggerClickHouseLoad(project, AUTO_MODEL_STAPLES_1, "ADMIN",
                        dataflowManager.getDataflow(AUTO_MODEL_STAPLES_1).getSegments().stream()
                                .map(NDataSegment::getId).collect(Collectors.toList())));

        // For historical reasons, the innerExpression of ComputedColumn is not standardized
        modelManager.listAllModels().forEach(model -> {
            if (model.isBroken()) {
                return;
            }
            List<ComputedColumnDesc> ccList = model.getComputedColumnDescs();
            for (ComputedColumnDesc ccDesc : ccList) {
                String innerExp = PushDownUtil.massageComputedColumn(model, model.getProject(), ccDesc, null);
                ccDesc.setInnerExpression(innerExp);
            }
        });

        // check
        test.checkHttpServer();
        test.overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");
        test.overwriteSystemProp("kylin.second-storage.query-pushdown-limit", "0");

        JdbcDatabaseContainer<?> clickhouse = clickHouse.getClickhouse(0);
        final SparkSession sparkSession = sharedSpark.getSpark();
        sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog,
                "org.apache.spark.sql.execution.datasources.jdbc.v2.SecondStorageCatalog");
        sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog + ".driver",
                clickhouse.getDriverClassName());
        // sparkSession.sessionState().conf().setConfString("spark.sql.ansi.enabled", "true");
    }

    @AfterClass
    public static void afterClass() {
        Unsafe.clearProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG);
    }

    private final String testName;
    private final String inputSqlPath;
    private final String resultPath;

    public TDVTTest(String testName, String sqlPath, String resultPath) {
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
        String resultTableIndex = runWithTableIndex(sqlStatement);
        log.info("SQL:{}", sqlStatement);
        Assert.assertEquals(resultTableIndex, resultPush);
        Assert.assertTrue(true);
    }

    private String runWithAggPushDown(String sqlStatement) throws Exception {
        QueryContext.current().setForceTableIndex(false);
        Dataset<Row> plan = ExecAndComp.queryModelWithoutCompute(project, sqlStatement);
        JDBCScan jdbcScan = ClickHouseUtils.findJDBCScan(plan.queryExecution().optimizedPlan());
        Assert.assertNotNull(jdbcScan);
        QueryContext.reset();
        return computeResult(plan);
    }

    private static String computeResult(Dataset<Row> plan) {
        return HiveResult$.MODULE$.hiveResultString(plan.queryExecution().executedPlan())
                .sorted(Ordering.String$.MODULE$).mkString("\n");
    }

    private String runWithTableIndex(String sqlStatement) throws Exception {
        try {
            QueryContext.current().setRetrySecondStorage(false);
            Dataset<Row> plan = ExecAndComp.queryModelWithoutCompute(project, sqlStatement);
            FilePruner filePruner = ClickHouseUtils.findFilePruner(plan.queryExecution().optimizedPlan());
            Assert.assertNotNull(filePruner);
            return computeResult(plan);
        } finally {
            QueryContext.current().setRetrySecondStorage(true);
        }
    }

    private static String triggerClickHouseLoad(String project, String modelId, String userName, List<String> segIds) {
        AbstractJobHandler localHandler = new SecondStorageSegmentLoadJobHandler();
        JobParam jobParam = SecondStorageJobParamUtil.of(project, modelId, userName, segIds.stream());
        ExecutableUtil.computeParams(jobParam);
        localHandler.handle(jobParam);
        return jobParam.getJobId();
    }

    private static void waitFinish(String project, String jobId) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NExecutableManager executableManager = NExecutableManager.getInstance(config, project);
        DefaultExecutable job = (DefaultExecutable) executableManager.getJob(jobId);
        await().atMost(300, TimeUnit.SECONDS).until(() -> !job.getStatus().isProgressing());
        Assert.assertFalse(job.getStatus().isProgressing());
        if (!Objects.equals(job.getStatus(), ExecutableState.SUCCEED)) {
            Assert.fail(firstFailedJobErrorMessage(executableManager, job));
        }
    }
}

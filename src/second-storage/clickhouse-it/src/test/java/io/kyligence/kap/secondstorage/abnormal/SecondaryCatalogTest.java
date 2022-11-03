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

import java.util.Collections;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
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
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;

public class SecondaryCatalogTest implements JobWaiter {
    static private final String cubeName = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";
    static private final String project = "table_index";

    @ClassRule
    public static SharedSparkSession sharedSpark = new SharedSparkSession(
            ImmutableMap.of("spark.sql.extensions", "io.kyligence.kap.query.SQLPushDownExtensions"));
    @ClassRule
    public static ClickHouseClassRule clickHouseClassRule = new ClickHouseClassRule(1);
    public EnableTestUser enableTestUser = new EnableTestUser();
    public EnableClickHouseJob test = new EnableClickHouseJob(clickHouseClassRule.getClickhouse(), 1, project,
            Collections.singletonList(cubeName), "src/test/resources/ut_meta");
    @Rule
    public TestRule rule = RuleChain.outerRule(enableTestUser).around(test);
    private final SparkSession sparkSession = sharedSpark.getSpark();

    /**
     * When set spark.sql.catalog.{queryCatalog}.url to an irrelevant clickhouse JDBC URL, there is an bug before
     * KE-27650
     */
    @Test
    public void testSparkJdbcUrlNotExist() throws Exception {
        final String queryCatalog = "xxxxx";
        try (JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            Unsafe.setProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG, queryCatalog);

            //build
            buildModel();
            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            Assert.assertEquals(3, SecondStorageUtil.setSecondStorageSizeInfo(modelManager.listAllModels()).size());

            // check
            test.checkHttpServer();
            test.overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");

            sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog,
                    "org.apache.spark.sql.execution.datasources.jdbc.v2.SecondStorageCatalog");
            sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog + ".url",
                    clickhouse2.getJdbcUrl());
            sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog + ".driver",
                    clickhouse2.getDriverClassName());
            sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog + ".pushDownAggregate",
                    "true");
            sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog + ".numPartitions",
                    "1");

            Dataset<Row> groupPlan = ExecAndComp.queryModelWithoutCompute(project,
                    "select sum(PRICE) from TEST_KYLIN_FACT group by PRICE");
            JDBCScan JdbcScan = ClickHouseUtils.findJDBCScan(groupPlan.queryExecution().optimizedPlan());
            Assert.assertEquals(1, JdbcScan.relation().parts().length);
            ClickHouseUtils.checkAggregateRemoved(groupPlan);
            String[] expectedPlanFragment = new String[] {
                    "PushedAggregates: [SUM(" + columnMapping.get("PRICE") + ")], ", "PushedFilters: [], ",
                    "PushedGroupByExpressions: [" + columnMapping.get("PRICE") + "], " };
            ClickHouseUtils.checkPushedInfo(groupPlan, expectedPlanFragment);
        } finally {
            Unsafe.clearProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG);
        }
    }

    public void buildModel() throws Exception {
        new IndexDataConstructor(project).buildDataflow(cubeName);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        waitJobFinish(project, triggerClickHouseLoadJob(project, cubeName, "ADMIN", dataflowManager
                .getDataflow(cubeName).getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList())));
    }
}

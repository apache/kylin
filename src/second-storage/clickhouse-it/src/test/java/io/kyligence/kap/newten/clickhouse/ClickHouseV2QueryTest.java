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
package io.kyligence.kap.newten.clickhouse;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.utils.RichOption;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Sort;
import org.apache.spark.sql.execution.datasources.jdbc.ClickHouseDialect$;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.ShardOptions$;
import org.apache.spark.sql.execution.datasources.v2.PostV2ScanRelationPushDown$;
import org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan;
import org.apache.spark.sql.execution.datasources.v2.jdbc.ShardJDBCTableCatalog;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.jdbc.JdbcDialects$;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.MetadataBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.google.common.collect.ImmutableList;

import lombok.extern.slf4j.Slf4j;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;

@Slf4j
public class ClickHouseV2QueryTest extends NLocalWithSparkSessionTest {

    private static final String table = ClickHouseUtils.PrepareTestData.db + "."
            + ClickHouseUtils.PrepareTestData.table;

    /**
     * According to JUnit's mechanism, the super class's method will be hidden by the child class for the same
     * Method signature. So we use {@link #beforeClass()} to hide {@link NLocalWithSparkSessionTest#beforeClass()}
     */
    @BeforeClass
    public static void beforeClass() {
        JdbcDialects$.MODULE$.registerDialect(ClickHouseDialect$.MODULE$);
        NLocalWithSparkSessionTest.ensureSparkConf();
        ClickHouseUtils.InjectNewPushDownRule(sparkConf);
        NLocalWithSparkSessionTest.beforeClass();
        Assert.assertTrue(SparderEnv.getSparkSession().sessionState().optimizer().preCBORules()
                .contains(V2ScanRelationPushDown$.MODULE$));
        Assert.assertTrue(SparderEnv.getSparkSession().sessionState().optimizer().preCBORules()
                .contains(PostV2ScanRelationPushDown$.MODULE$));
    }

    @AfterClass
    public static void afterClass() {
        NLocalWithSparkSessionTest.afterClass();
        JdbcDialects$.MODULE$.unregisterDialect(ClickHouseDialect$.MODULE$);
    }

    static private void setupCatalog(JdbcDatabaseContainer<?> clickhouse, String catalogPrefix) {
        SQLConf conf = SparderEnv.getSparkSession().sessionState().conf();
        conf.setConfString(SQLConf.ANSI_ENABLED().key(), "true");
        conf.setConfString(catalogPrefix, ShardJDBCTableCatalog.class.getCanonicalName());
        conf.setConfString(catalogPrefix + ".url", clickhouse.getJdbcUrl());
        conf.setConfString(catalogPrefix + ".driver", clickhouse.getDriverClassName());
        conf.setConfString(catalogPrefix + ".pushDownLimit", "true");
        conf.setConfString(catalogPrefix + ".pushDownAggregate", "true");
        conf.setConfString(catalogPrefix + ".pushDownOffset", "true");
    }

    private void executeAndCheck(Dataset<Row> dataset, List<Row> expectedRow, int expectedShards) {
        List<Row> results1 = dataset.collectAsList();
        Assert.assertEquals(expectedRow, results1);

        JDBCScan jdbcScan = ClickHouseUtils.findJDBCScan(dataset.queryExecution().optimizedPlan());
        Assert.assertEquals(expectedShards, jdbcScan.relation().parts().length);
    }

    private void checkFiltersRemoved(Dataset ds) {
        Optional<Filter> optional = new RichOption<>(
                ds.queryExecution().optimizedPlan().find(new AbstractFunction1<LogicalPlan, Object>() {
                    @Override
                    public Object apply(LogicalPlan v1) {
                        return v1 instanceof Filter;
                    }
                })).toOptional().map(logical -> (Filter) logical);
        assert Optional.empty().equals(optional);
    }

    private void checkSortRemoved(Dataset ds, boolean removed) {
        Optional<Sort> optional = new RichOption<>(
                ds.queryExecution().optimizedPlan().find(new AbstractFunction1<LogicalPlan, Object>() {
                    @Override
                    public Object apply(LogicalPlan s) {
                        return s instanceof Sort;
                    }
                })).toOptional().map(logical -> (Sort) logical);
        if (removed) {
            assert !optional.isPresent();
        } else {
            assert optional.isPresent();
        }
    }

    private void testFilter(String catalogName, int expectedShards) {
        String sql = String.format(Locale.ROOT, "select s2, i1, i2, n3 from %s.%s where i1 > 5 and n4 < 2 order by i1",
                catalogName, table);
        Dataset<Row> dataset = ss.sql(sql);
        checkFiltersRemoved(dataset);
        checkSortRemoved(dataset, false);
        String expectedPlanFragment = "PushedFilters: [i1 IS NOT NULL, n4 IS NOT NULL, i1 > 5, n4 < 2], ";
        ClickHouseUtils.checkPushedInfo(dataset, expectedPlanFragment);
        BigDecimal decimal = Decimal.apply(new BigDecimal(-18.22), 19, 4).toJavaBigDecimal();
        List<Row> expectedRow = ImmutableList.of(RowFactory.create("2", 6, 7L, decimal),
                RowFactory.create("4", 7, 3L, decimal));
        executeAndCheck(dataset, expectedRow, expectedShards);

        String sql2 = String.format(Locale.ROOT,
                "select s2, i1, i2, n3 from %s.%s where i1 > 5 and cast(n3 as double) < 2 order by i1", catalogName,
                table);
        Dataset<Row> dataset2 = ss.sql(sql2);
        checkFiltersRemoved(dataset2);
        checkSortRemoved(dataset2, false);
        String expectedPlanFragment2 = "PushedFilters: [i1 IS NOT NULL, n3 IS NOT NULL, i1 > 5, n3 < 2.0000], ";
        ClickHouseUtils.checkPushedInfo(dataset2, expectedPlanFragment2);
        List<Row> expectedRow2 = expectedRow;
        executeAndCheck(dataset2, expectedRow2, expectedShards);

        String sql3 = String.format(Locale.ROOT,
                "select s2, i1, i2, n3 from %s.%s where i1 > 5 and cast(str_date4 as date) > date'2021-01-06' order by i1",
                catalogName, table);
        Dataset<Row> dataset3 = ss.sql(sql3);
        checkFiltersRemoved(dataset3);
        checkSortRemoved(dataset3, false);
        String expectedPlanFragment3 = "PushedFilters: [i1 IS NOT NULL, str_date4 IS NOT NULL, i1 > 5, CAST(str_date4 AS date) > 18633], ";
        ClickHouseUtils.checkPushedInfo(dataset3, expectedPlanFragment3);
        List<Row> expectedRow3 = ImmutableList.of(RowFactory.create("2", 6, 7L, decimal));
        executeAndCheck(dataset3, expectedRow3, expectedShards);
    }

    private void testLimit(String catalogName, int expectedShards) {
        String sql = String.format(Locale.ROOT, "select s2, i1, i2 from %s.%s where i1 > 6 limit 1", catalogName,
                table);
        Dataset<Row> dataset = ss.sql(sql);
        String[] expectedPlanFragment = new String[] { "PushedFilters: [i1 IS NOT NULL, i1 > 6], ",
                "PushedLimit: LIMIT 1, " };
        ClickHouseUtils.checkPushedInfo(dataset, expectedPlanFragment);
        List<Row> expectedRow = ImmutableList.of(RowFactory.create("4", 7, 3L));
        executeAndCheck(dataset, expectedRow, expectedShards);
    }

    private void testOffset(String catalogName, int expectedShards) {
        String sql = String.format(Locale.ROOT, "select s2, i1, i2 from %s.%s where i1 > 6 offset 1", catalogName,
                table);
        Dataset<Row> dataset = ss.sql(sql);
        String[] expectedPlanFragment;
        if (expectedShards == 1) {
            expectedPlanFragment = new String[] { "PushedFilters: [i1 IS NOT NULL, i1 > 6],",
                    "PushedOffset: OFFSET 1, " };
        } else {
            expectedPlanFragment = new String[] { "PushedFilters: [i1 IS NOT NULL, i1 > 6]," };
        }
        ClickHouseUtils.checkPushedInfo(dataset, expectedPlanFragment);
        List<Row> expectedRow = ImmutableList.of();
        executeAndCheck(dataset, expectedRow, expectedShards);

        String sql1 = String.format(Locale.ROOT, "select sum(i2) from %s.%s where i1 > 6 group by i1 limit 1 offset 1",
                catalogName, table);
        Dataset<Row> dataset1 = ss.sql(sql1);
        String[] expectedPlanFragment1;
        if (expectedShards == 1) {
            expectedPlanFragment1 = new String[] { "PushedAggregates: [SUM(i2)], ",
                    "PushedFilters: [i1 IS NOT NULL, i1 > 6], ", "PushedGroupByExpressions: [i1], ",
                    "PushedLimit: LIMIT 2", "PushedOffset: OFFSET 1," };
        } else {
            expectedPlanFragment1 = new String[] { "PushedAggregates: [SUM(i2)], ",
                    "PushedFilters: [i1 IS NOT NULL, i1 > 6], ", "PushedGroupByExpressions: [i1], " };
        }
        ClickHouseUtils.checkPushedInfo(dataset1, expectedPlanFragment1);
        List<Row> expectedRow1 = ImmutableList.of();
        executeAndCheck(dataset1, expectedRow1, expectedShards);
    }

    private void testTop(String catalogName, int expectedShards) {
        String sql1 = String.format(Locale.ROOT, "select s2, i1, i2 from %s.%s where i1 > 1 order by i1 limit 1",
                catalogName, table);
        Dataset<Row> dataset1 = ss.sql(sql1);
        checkSortRemoved(dataset1, expectedShards == 1);
        String[] expectedPlanFragment1 = new String[] { "PushedFilters: [i1 IS NOT NULL, i1 > 1], ",
                "PushedTopN: ORDER BY [i1 ASC NULLS FIRST] LIMIT 1, " };
        ClickHouseUtils.checkPushedInfo(dataset1, expectedPlanFragment1);
        List<Row> expectedRow1 = ImmutableList.of(RowFactory.create("3", 2, 3L));
        executeAndCheck(dataset1, expectedRow1, expectedShards);

        // Sort with alias in TopN
        String sql2 = String.format(Locale.ROOT,
                "select s2, i1 as my_i1, i2 from %s.%s where i1 > 1 order by my_i1 limit 1", catalogName, table);
        Dataset<Row> dataset2 = ss.sql(sql2);
        checkSortRemoved(dataset2, expectedShards == 1);
        String[] expectedPlanFragment2 = new String[] { "PushedFilters: [i1 IS NOT NULL, i1 > 1], ",
                "PushedTopN: ORDER BY [i1 ASC NULLS FIRST] LIMIT 1, " };
        ClickHouseUtils.checkPushedInfo(dataset2, expectedPlanFragment2);
        List<Row> expectedRow2 = expectedRow1;
        executeAndCheck(dataset2, expectedRow2, expectedShards);

        // Sort with agg in TopN
        String sql3 = String.format(Locale.ROOT,
                "select sum(i2) from %s.%s where i1 > 1 group by i1 order by i1 limit 1", catalogName, table);
        Dataset<Row> dataset3 = ss.sql(sql3);
        checkSortRemoved(dataset3, expectedShards == 1);
        String[] expectedPlanFragment3;
        if (expectedShards == 1) {
            expectedPlanFragment3 = new String[] { "PushedAggregates: [SUM(i2)],",
                    "PushedFilters: [i1 IS NOT NULL, i1 > 1],", "PushedGroupByExpressions: [i1],",
                    "PushedTopN: ORDER BY [i1 ASC NULLS FIRST] LIMIT 1" };
        } else {
            expectedPlanFragment3 = new String[] { "PushedAggregates: [SUM(i2)],",
                    "PushedFilters: [i1 IS NOT NULL, i1 > 1],", "PushedGroupByExpressions: [i1]," };
        }
        ClickHouseUtils.checkPushedInfo(dataset3, expectedPlanFragment3);
        List<Row> expectedRow3 = ImmutableList.of(RowFactory.create(3));
        executeAndCheck(dataset3, expectedRow3, expectedShards);
    }

    private void testPaging(String catalogName, int expectedShards) {
        // Paging without sort
        String sql1 = String.format(Locale.ROOT, "select s2, i1, i2 from %s.%s where i1 > 1 limit 1 offset 1",
                catalogName, table);
        Dataset<Row> dataset1 = ss.sql(sql1);
        String[] expectedPlanFragment1 = new String[] { "PushedFilters: [i1 IS NOT NULL, i1 > 1], ",
                "PushedLimit: LIMIT 2, " };
        ClickHouseUtils.checkPushedInfo(dataset1, expectedPlanFragment1);

        // Paging with sort
        String sql2 = String.format(Locale.ROOT,
                "select s2, i1, i2 from %s.%s where i1 > 1 order by i1 limit 1 offset 1", catalogName, table);
        Dataset<Row> dataset2 = ss.sql(sql2);
        checkSortRemoved(dataset2, expectedShards == 1);
        String[] expectedPlanFragment2 = new String[] { "PushedFilters: [i1 IS NOT NULL, i1 > 1], ",
                "PushedTopN: ORDER BY [i1 ASC NULLS FIRST] LIMIT 2, " };
        ClickHouseUtils.checkPushedInfo(dataset2, expectedPlanFragment2);
        List<Row> expectedRow2 = ImmutableList.of(RowFactory.create("3", 3, 4L));
        executeAndCheck(dataset2, expectedRow2, expectedShards);

        String sql3 = String.format(Locale.ROOT,
                "select s2, i1 as my_i1, i2 from %s.%s where i1 > 1 order by my_i1 limit 1 offset 1", catalogName,
                table);
        Dataset<Row> dataset3 = ss.sql(sql3);
        checkSortRemoved(dataset3, expectedShards == 1);
        String[] expectedPlanFragment3 = new String[] { "PushedFilters: [i1 IS NOT NULL, i1 > 1], ",
                "PushedTopN: ORDER BY [i1 ASC NULLS FIRST] LIMIT 2, " };
        ClickHouseUtils.checkPushedInfo(dataset3, expectedPlanFragment3);
        List<Row> expectedRow3 = expectedRow2;
        executeAndCheck(dataset3, expectedRow3, expectedShards);

        String sql4 = String.format(Locale.ROOT, "select s2, i1, i2 from %s.%s order by i1 limit 2 offset 1",
                catalogName, table);
        Dataset<Row> dataset4 = ss.sql(sql4);
        checkSortRemoved(dataset4, expectedShards == 1);
        String[] expectedPlanFragment4 = new String[] { "PushedFilters: [], ",
                "PushedTopN: ORDER BY [i1 ASC NULLS FIRST] LIMIT 3, " };
        ClickHouseUtils.checkPushedInfo(dataset4, expectedPlanFragment4);
        List<Row> expectedRow4 = ImmutableList.of(RowFactory.create("3", 2, 3L), RowFactory.create("3", 3, 4L));
        executeAndCheck(dataset4, expectedRow4, expectedShards);

        String sql5 = String.format(Locale.ROOT,
                "select s2, i1 as my_i1, i2 from %s.%s order by my_i1 limit 2 offset 1", catalogName, table);
        Dataset<Row> dataset5 = ss.sql(sql5);
        checkSortRemoved(dataset5, expectedShards == 1);
        String[] expectedPlanFragment5 = new String[] { "PushedFilters: [], ",
                "PushedTopN: ORDER BY [i1 ASC NULLS FIRST] LIMIT 3, " };
        ClickHouseUtils.checkPushedInfo(dataset5, expectedPlanFragment5);
        List<Row> expectedRow5 = expectedRow4;
        executeAndCheck(dataset5, expectedRow5, expectedShards);
    }

    private void testAggregate(String catalogName, int expectedShards) {
        String sql = String.format(Locale.ROOT,
                "select s2, sum(i1), sum(i2), count(i1), count(*) from %s.%s group by s2 order by s2", catalogName,
                table);
        Dataset<Row> dataset = ss.sql(sql);
        checkSortRemoved(dataset, false);
        ClickHouseUtils.checkAggregateRemoved(dataset, expectedShards == 1);
        String[] expectedPlanFragment = new String[] { "PushedAggregates: [SUM(i1), SUM(i2), COUNT(i1), COUNT(*)], ",
                "PushedFilters: [], ", "PushedGroupByExpressions: [s2], " };
        ClickHouseUtils.checkPushedInfo(dataset, expectedPlanFragment);
        List<Row> expectedRow = ImmutableList.of(RowFactory.create("2", 12, 15L, 3L, 3L),
                RowFactory.create("3", 9, 12L, 3L, 3L), RowFactory.create("4", 7, 3L, 1L, 1L));
        executeAndCheck(dataset, expectedRow, expectedShards);

        String sql2 = "select s2," + " COUNT(CASE WHEN i1 > 1 AND i1 < 3 THEN i1 ELSE 0 END),"
                + " COUNT(CASE WHEN i1 >= 1 OR i1 <= 3 THEN i1 ELSE 0 END),"
                + " MAX(CASE WHEN NOT(i1 > 1) AND NOT(i1 < 3) THEN i1 ELSE 0 END),"
                + " MAX(CASE WHEN NOT(i1 != 0) OR NOT(i1 < 1) THEN i1 ELSE 0 END),"
                + " MIN(CASE WHEN NOT(i1 > 1 OR i1 IS NULL) THEN i1 ELSE 0 END),"
                + " SUM(CASE WHEN NOT(i1 > 1 AND i1 IS NOT NULL) THEN i1 ELSE 0 END),"
                + " SUM(CASE WHEN i1 > 2 THEN 2 WHEN i1 > 1 THEN 1 END),"
                + " AVG(CASE WHEN NOT(i1 > 1 OR i1 IS NOT NULL) THEN i1 ELSE 0 END)"
                + String.format(Locale.ROOT, " from %s.%s group by s2 order by s2", catalogName, table);
        Dataset<Row> dataset2 = ss.sql(sql2);
        checkSortRemoved(dataset2, false);
        ClickHouseUtils.checkAggregateRemoved(dataset2, expectedShards == 1);
        String[] expectedPlanFragment2 = new String[] {
                "PushedAggregates: [COUNT(CASE WHEN (i1 > 1) AND (i1 < 3) THEN i1 ELSE 0 END), COUNT(CASE WHEN (i1 >= 1) OR (i1 <= 3..., ",
                "PushedFilters: [], ", "PushedGroupByExpressions: [s2], " };
        ClickHouseUtils.checkPushedInfo(dataset2, expectedPlanFragment2);
        List<Row> expectedRow2 = ImmutableList.of(RowFactory.create("2", 3, 3, 0, 6, 0, 1, 4, 0),
                RowFactory.create("3", 3, 3, 0, 4, 0, 0, 5, 0), RowFactory.create("4", 1, 1, 0, 7, 0, 0, 2, 0));
        executeAndCheck(dataset2, expectedRow2, expectedShards);

        String sql3 = String.format(Locale.ROOT,
                "select s2, sum(distinct i1), count(distinct i1), avg(distinct i1) from %s.%s group by s2 order by s2",
                catalogName, table);
        Dataset<Row> dataset3 = ss.sql(sql3);
        checkSortRemoved(dataset3, false);
        ClickHouseUtils.checkAggregateRemoved(dataset3, expectedShards == 1);
        String[] expectedPlanFragment3;
        if (expectedShards == 1) {
            expectedPlanFragment3 = new String[] {
                    "PushedAggregates: [SUM(DISTINCT i1), COUNT(DISTINCT i1), AVG(DISTINCT i1)], ",
                    "PushedFilters: [], ", "PushedGroupByExpressions: [s2], " };
        } else {
            expectedPlanFragment3 = new String[] { "PushedFilters: [], " };
        }
        ClickHouseUtils.checkPushedInfo(dataset3, expectedPlanFragment3);
        List<Row> expectedRow3 = ImmutableList.of(RowFactory.create("2", 12, 3, 4.0), RowFactory.create("3", 9, 3, 3.0),
                RowFactory.create("4", 7, 1, 7.0));
        executeAndCheck(dataset3, expectedRow3, expectedShards);
    }

    @Test
    public void testOnSingleShard() throws Exception {
        boolean result = ClickHouseUtils.prepare1Instance(true,
                (JdbcDatabaseContainer<?> clickhouse, Connection connection) -> {

                    final String catalogName = "testOnSingleShard";
                    final String catalogPrefix = "spark.sql.catalog." + catalogName;
                    setupCatalog(clickhouse, catalogPrefix);

                    testFilter(catalogName, 1);
                    testLimit(catalogName, 1);
                    testTop(catalogName, 1);
                    testPaging(catalogName, 1);
                    testAggregate(catalogName, 1);
                    testOffset(catalogName, 1);

                    return true;
                });
        Assert.assertTrue(result);
    }

    @Test
    public void testOnMultipleShard() throws Exception {
        boolean result = ClickHouseUtils.prepare2Instances(true, (JdbcDatabaseContainer<?> clickhouse1,
                Connection connection1, JdbcDatabaseContainer<?> clickhouse2, Connection connection2) -> {
            final String catalogName = "testOnMultipleShard";
            final String catalogPrefix = "spark.sql.catalog." + catalogName;
            List<String> shardList = ImmutableList.of(clickhouse1.getJdbcUrl(), clickhouse2.getJdbcUrl());
            String shards = ShardOptions$.MODULE$.buildSharding(JavaConverters.asScalaBuffer(shardList));
            setupCatalog(clickhouse1, catalogPrefix);
            SparderEnv.getSparkSession().sessionState().conf()
                    .setConfString(catalogPrefix + "." + ShardOptions$.MODULE$.SHARD_URLS(), shards);
            SparderEnv.getSparkSession().sessionState().conf().setConfString(
                    catalogPrefix + "." + JDBCOptions.JDBC_NUM_PARTITIONS(), String.valueOf(shardList.size()));

            testFilter(catalogName, shardList.size());
            testLimit(catalogName, shardList.size());
            testTop(catalogName, shardList.size());
            testPaging(catalogName, shardList.size());
            testAggregate(catalogName, shardList.size());
            testOffset(catalogName, shardList.size());

            return true;
        });
        Assert.assertTrue(result);
    }

    @Test
    public void testMultipleShardWithDataFrame() throws Exception {
        ClickHouseUtils.prepare2Instances(true, (JdbcDatabaseContainer<?> clickhouse1, Connection connection1,
                JdbcDatabaseContainer<?> clickhouse2, Connection connection2) -> {
            final String catalogName = "testMultipleShardWithDataFrame";
            final String catalogPrefix = "spark.sql.catalog." + catalogName;
            List<String> shardList = ImmutableList.of(clickhouse1.getJdbcUrl(), clickhouse2.getJdbcUrl());
            String shards = ShardOptions$.MODULE$.buildSharding(JavaConverters.asScalaBuffer(shardList));
            setupCatalog(clickhouse1, catalogPrefix);
            DataFrameReader reader = new DataFrameReader(SparderEnv.getSparkSession());
            reader.option(ShardOptions$.MODULE$.SHARD_URLS(), shards);
            reader.option(JDBCOptions.JDBC_NUM_PARTITIONS(), shardList.size());
            Dataset<Row> df = reader.table(catalogName + "." + table);
            Assert.assertEquals(7, df.count());
            return true;
        });
    }

    @Test
    public void testClickhouseType() {
        JdbcDialect dialect = JdbcDialects.get("jdbc:clickhouse");
        MetadataBuilder md = new MetadataBuilder().putString("name", "test_column").putLong("scale", -127);
        Assert.assertNotNull(dialect.getCatalystType(java.sql.Types.ARRAY, "Array(_numeric)", 0, md));
    }
}

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

package org.apache.kylin.auto;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.util.ExecAndComp.CompareLevel;
import org.apache.kylin.util.ExecAndCompExt;
import org.apache.kylin.util.MetadataTestUtils;
import org.apache.kylin.util.SuggestTestBase;
import org.apache.spark.sql.SparderEnv;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import lombok.extern.slf4j.Slf4j;

/**
 *
 * used for test scenarios that mainly caused by all kinds of sql,
 * So it ensures that these sqls can be auto-propose correctly and
 * get right result from the pre-calculate layout.
 *
 */
@Slf4j
@SuppressWarnings("squid:S2699")
@RunWith(Parameterized.class)
public class NAutoBuildAndQueryTest extends SuggestTestBase {

    @Parameterized.Parameters
    public static Integer[] storageTypes() {
        return new Integer[] { 1, 3 };
    }

    private final int storageType;

    public NAutoBuildAndQueryTest(int storageType) {
        this.storageType = storageType;
    }

    @Test
    public void testSumExpr() throws Exception {
        excludedSqlPatterns.addAll(loadWhiteListPatterns());
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.model.non-equi-join-recommendation-enabled", "TRUE");
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        overwriteSystemProp("kylin.query.convert-sum-expression-enabled", "TRUE");

        overwriteSystemProp("kylin.storage.columnar.spark-conf.spark.databricks.delta.snapshotPartitions", "1");
        overwriteSystemProp("kylin.engine.spark-conf.spark.databricks.delta.snapshotPartitions", "1");

        new TestScenario(CompareLevel.SAME, "query/sql_sum_expr").execute(storageType);
    }

    @Test
    public void testCountDistinctExpr() throws Exception {
        overwriteSystemProp("kylin.query.convert-sum-expression-enabled", "TRUE");
        overwriteSystemProp("kylin.query.convert-count-distinct-expression-enabled", "TRUE");

        new TestScenario(CompareLevel.SAME, "query/sql_count_distinct_expr").execute(storageType);
    }

    @Test
    public void testCountDBitmap() throws Exception {
        overwriteSystemProp("kylin.query.convert-sum-expression-enabled", "TRUE");
        overwriteSystemProp("kylin.query.convert-count-distinct-expression-enabled", "TRUE");
        new TestScenario(CompareLevel.SAME, "query/sql_countd_bitmap").execute();
    }

    @Test
    public void testDimensionAsMeasure() throws Exception {
        MetadataTestUtils.updateProjectConfig(getProject(), "kylin.query.implicit-computed-column-convert", "FALSE");
        new TestScenario(CompareLevel.SAME, "query/sql_dimension_as_measure").execute(storageType);

        Map<String, String> props = ImmutableMap.<String, String> builder()
                .put("kylin.query.implicit-computed-column-convert", "FALSE")
                .put("kylin.query.convert-sum-expression-enabled", "TRUE").build();
        MetadataTestUtils.updateProjectConfig(getProject(), props);
        new TestScenario(CompareLevel.SAME, "query/sql_dimension_as_measure").execute(storageType);

        Map<String, String> props2 = ImmutableMap.<String, String> builder()
                .put("kylin.query.implicit-computed-column-convert", "FALSE")
                .put("kylin.query.convert-sum-expression-enabled", "FALSE")
                .put("kylin.query.convert-count-distinct-expression-enabled", "TRUE").build();
        MetadataTestUtils.updateProjectConfig(getProject(), props2);
        new TestScenario(CompareLevel.SAME, "query/sql_dimension_as_measure").execute(storageType);
    }

    @Test
    public void testAllQueries() throws Exception {
        ExecAndCompExt.currentTestGlutenDisabledSqls
                .set(Sets.newHashSet("src/test/resources/query/sql_computedcolumn/sql_ccv2/04.sql", // round accuracy
                        "src/test/resources/query/sql_like/query25.sql", // like escape unsupported
                        "src/test/resources/query/sql_like/query26.sql", // like escape unsupported
                        "src/test/resources/query/sql_truncate/query02.sql", // truncate double/float accuracy
                        "src/test/resources/query/sql_truncate/query03.sql", // truncate double/float accuracy
                        "src/test/resources/query/sql_truncate/query04.sql", // truncate double/float accuracy
                        "src/test/resources/query/sql_truncate/query05.sql", // truncate double/float accuracy
                        "src/test/resources/query/sql_truncate/query06.sql", // truncate double/float accuracy, cast decimal to tinyint overflow
                        "src/test/resources/query/sql_truncate/query07.sql", // truncate double/float accuracy, cast decimal to tinyint overflow
                        "src/test/resources/query/sql_spark_func/math/rint-01.sql", // rint not support
                        "src/test/resources/query/sql_spark_func/math/rint-02.sql", // rint not support
                        "src/test/resources/query/sql_spark_func/math/rint-03.sql" // rint not support
                ));
        excludedSqlPatterns.addAll(loadWhiteListPatterns());
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.model.non-equi-join-recommendation-enabled", "TRUE");
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        overwriteSystemProp("kylin.query.join-match-optimization-enabled", "TRUE");

        MetadataTestUtils.updateProjectConfig(getProject(), "kylin.query.metadata.expose-computed-column", "FALSE");

        executeTestScenario(storageType, 22,
                /* CompareLevel = SAME */
                new TestScenario(CompareLevel.SAME, "query/h2"), //
                new TestScenario(CompareLevel.SAME, "query/sql_replace_special_symbol"), //
                new TestScenario(CompareLevel.SAME, "query/sql"), //
                new TestScenario(CompareLevel.SAME, "query/sql_boolean"), //
                new TestScenario(CompareLevel.SAME, "query/sql_cache"), //
                new TestScenario(CompareLevel.SAME, "query/sql_casewhen"), //
                new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn/sql_ccv2"), //
                new TestScenario(CompareLevel.SAME, "query/sql_constant"), //
                new TestScenario(CompareLevel.SAME, "query/sql_cross_join"), //
                new TestScenario(CompareLevel.SAME, "query/sql_current_date"), //
                new TestScenario(CompareLevel.SAME, "query/sql_datetime"), //
                new TestScenario(CompareLevel.SAME, "query/sql_day_of_week"), //
                new TestScenario(CompareLevel.SAME, "query/sql_derived"), //
                new TestScenario(CompareLevel.SAME, "query/sql_distinct"), //
                new TestScenario(CompareLevel.SAME, "query/sql_distinct_dim"), //
                new TestScenario(CompareLevel.SAME, "query/sql_except"),
                new TestScenario(CompareLevel.SAME, "query/sql_extended_column"), //
                new TestScenario(CompareLevel.SAME, "query/sql_filter_simplify"), //
                new TestScenario(CompareLevel.SAME, "query/sql_grouping"), //
                new TestScenario(CompareLevel.SAME, "query/sql_hive"), //
                new TestScenario(CompareLevel.SAME, "query/sql_in"), //
                new TestScenario(CompareLevel.SAME, "query/sql_inner_column"), //
                new TestScenario(CompareLevel.SAME, "query/sql_join"), //
                new TestScenario(CompareLevel.SAME, "query/sql_join/sql_right_join"), //
                new TestScenario(CompareLevel.SAME, "query/sql_kap"), //
                new TestScenario(CompareLevel.SAME, "query/sql_like"), //
                new TestScenario(CompareLevel.SAME, "query/sql_lookup"), //
                new TestScenario(CompareLevel.SAME, "query/sql_magine"), //
                new TestScenario(CompareLevel.SAME, "query/sql_magine_inner"), //
                new TestScenario(CompareLevel.SAME, "query/sql_magine_left"), //
                new TestScenario(CompareLevel.SAME, "query/sql_multi_model"), //
                new TestScenario(CompareLevel.SAME, "query/sql_probe"), //
                new TestScenario(CompareLevel.SAME, "query/sql_raw"), //
                new TestScenario(CompareLevel.SAME, "query/sql_rawtable"), //
                new TestScenario(CompareLevel.SAME, "query/sql_similar"), //
                new TestScenario(CompareLevel.SAME, "query/sql_snowflake"), //
                new TestScenario(CompareLevel.SAME, "query/sql_subquery"), //
                new TestScenario(CompareLevel.SAME, "query/sql_string"), //
                new TestScenario(CompareLevel.SAME, "query/sql_tableau"), //
                new TestScenario(CompareLevel.SAME, "query/sql_timestamp"), //
                new TestScenario(CompareLevel.SAME, "query/sql_udf"), //
                new TestScenario(CompareLevel.SAME, "query/sql_union"), //
                new TestScenario(CompareLevel.SAME, "query/sql_union_cache"), //
                new TestScenario(CompareLevel.SAME, "query/sql_value"), //
                new TestScenario(CompareLevel.SAME, "query/sql_verifyContent"), //
                new TestScenario(CompareLevel.SAME, "query/sql_window/new_sql_window"), //
                new TestScenario(CompareLevel.SAME, "query/sql_spark_func/time"),
                new TestScenario(CompareLevel.SAME, "query/sql_spark_func/string"),
                new TestScenario(CompareLevel.SAME, "query/sql_spark_func/misc"),
                new TestScenario(CompareLevel.SAME, "query/sql_spark_func/math"),
                new TestScenario(CompareLevel.SAME, "query/sql_spark_func/constant_query"),
                /*
                Because of spark.sql.legacy.timeParserPolicy=LEGACY in sparder or job engine,
                date_format() function has inaccurate results when formatting millisecond values.
                |-----------------------|------------------------- Pattern ----------------------------|
                |-------- Time ---------|-------- SSS -------|------- SS -------|--------- S ----------|
                |1002-12-01 01:02:03.123|         123        |        123       |          123         |
                |1002-12-01 01:02:03.012|         012        |        12        |          12          |
                |1002-12-01 01:02:03.001|         001        |        01        |          1           |
                |1002-12-01 01:02:03.000|         000        |        00        |          0           |
                |1002-12-01 01:02:03    |         000        |        00        |          0           |
                 */
                new TestScenario(CompareLevel.SAME, "query/sql_spark_func/format"),
                new TestScenario(CompareLevel.SAME, "query/sql_truncate"), //
                new TestScenario(CompareLevel.SAME, "query/sql-replace"), //
                new TestScenario(CompareLevel.SAME, "query/sql_spark_func/null"),

                /* CompareLevel = SAME, JoinType = LEFT */
                new TestScenario(CompareLevel.SAME, JoinType.LEFT, "query/sql_distinct_precisely"), //
                new TestScenario(CompareLevel.SAME, JoinType.LEFT, "query/sql_topn"), //

                /* CompareLevel = SAME_ROWCOUNT */
                new TestScenario(CompareLevel.SAME_ROWCOUNT,
                        "query/sql_computedcolumn/sql_computedcolumn_ifnull_timestamp"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_distinct/sql_distinct_hllc"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_function/sql_function_ifnull_timestamp"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_function/sql_function_constant_func"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_h2_uncapable"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_limit"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_percentile"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_percentile_only_with_spark_cube"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_verifyCount"),

                /* CompareLevel = SAME_ORDER */
                new TestScenario(CompareLevel.SAME_ORDER, "query/sql_orderby"), //
                new TestScenario(CompareLevel.SAME_ORDER, "query/sql_window"),

                /* CompareLevel = NONE */
                new TestScenario(CompareLevel.NONE, "query/sql_intersect_count"),
                new TestScenario(CompareLevel.NONE, "query/sql_limit_offset"),
                new TestScenario(CompareLevel.NONE, "query/sql_function/sql_function_round"));

        ExecAndCompExt.currentTestGlutenDisabledSqls.remove();
    }

    @Test
    public void testBuildAndQueryWithExcludeTable() throws Exception {
        excludedSqlPatterns.addAll(loadWhiteListPatterns());
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.model.non-equi-join-recommendation-enabled", "TRUE");
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        executeTestScenario(BuildAndCompareContext.builder().expectModelNum(2)
                .testScenarios(Lists.newArrayList(new TestScenario(CompareLevel.SAME, "query/h2"))) //
                .storageType(storageType).extension(df -> {
                    EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(),
                                getProject());
                        TableExtDesc tableExt = tableMgr.getOrCreateTableExt(df.getModel().getRootFactTableName());
                        tableExt.setExcluded(true);
                        tableMgr.saveOrUpdateTableExt(false, tableExt);
                        return null;
                    }, getProject());
                }).build());
    }

    @Test
    public void testSpecialJoin() throws Exception {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.model.non-equi-join-recommendation-enabled", "TRUE");
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        MetadataTestUtils.updateProjectConfig(getProject(), "kylin.query.metadata.expose-computed-column", "TRUE");

        executeTestScenarioWithStorageType(storageType, new TestScenario(CompareLevel.SAME, "query/sql_powerbi"),
                new TestScenario(CompareLevel.SAME, "query/sql_special_join"));

        MetadataTestUtils.updateProjectConfig(getProject(), "kylin.query.metadata.expose-computed-column", "FALSE");

        executeTestScenarioWithStorageType(storageType,
                new TestScenario(CompareLevel.SAME, "query/sql_special_join_condition"));
    }

    // combine this case with queries under sql_powerbi to learn OlapJoinRule
    @Test
    public void testNonEqualJoin() throws Exception {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.model.non-equi-join-recommendation-enabled", "TRUE");
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        String folder = "query/sql_non_equi_join";

        executeTestScenarioWithStorageType(storageType, new TestScenario(CompareLevel.SAME, folder, 0, 10),
                new TestScenario(CompareLevel.SAME, folder, 12, 27),
                new TestScenario(CompareLevel.SAME, folder, 29, 35));

        // select star has some known problem
        // many-to-many merge left joins gives wrong result
        dropAllModels();
        executeTestScenarioWithStorageType(storageType, new TestScenario(CompareLevel.SAME, folder, 27, 29),
                new TestScenario(CompareLevel.SAME, folder, 10, 11));

        // many-to-many merge left joins gives wrong result
        dropAllModels();
        executeTestScenarioWithStorageType(storageType, new TestScenario(CompareLevel.SAME, folder, 11, 12));
    }

    @Test
    public void testNonEqualInnerJoin() throws Exception {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "FALSE");
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        Assert.assertFalse(KylinConfig.getInstanceFromEnv().isQueryNonEquiJoinModelEnabled());
        executeTestScenario(storageType, 2, new TestScenario(CompareLevel.SAME, "query/sql_non_equi_join", 32, 33));

        // scd2 join condition gives one model, but this query is just non-equiv-join
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.model.non-equi-join-recommendation-enabled", "TRUE");
        Assert.assertTrue(KylinConfig.getInstanceFromEnv().isQueryNonEquiJoinModelEnabled());
        executeTestScenario(storageType, 2, new TestScenario(CompareLevel.SAME, "query/sql_non_equi_join", 32, 33));
    }

    @Test
    public void testUDFs() throws Exception {
        overwriteSystemProp("kylin.query.print-logical-plan", "TRUE");
        ExecAndCompExt.currentTestGlutenDisabledSqls
                .set(Sets.newHashSet("src/test/resources/query/sql_function/query01.sql", // exp(n), n > 22 cause inaccuracy
                        "src/test/resources/query/sql_function/query03.sql", // initcap('fp-gtc'), 'Fp-gtc' vs 'Fp-Gtc'
                        "src/test/resources/query/sql_function/query04.sql", // initcap('fp-gtc'), 'Fp-gtc' vs 'Fp-Gtc'
                        "src/test/resources/query/sql_function/query06.sql", // initcap('fp-gtc'), 'Fp-gtc' vs 'Fp-Gtc'
                        "src/test/resources/query/sql_function/query20.sql", // initcap('fp-gtc'), 'Fp-gtc' vs 'Fp-Gtc'
                        "src/test/resources/query/sql_function/query21.sql", // initcap('fp-gtc'), 'Fp-gtc' vs 'Fp-Gtc'
                        "src/test/resources/query/sql_function/query32.sql", // timestamp accuracy
                        "src/test/resources/query/sql_function/query35.sql", // overlay function
                        "src/test/resources/query/sql_function/sql_function_formatUDF/query02.sql", // date_format
                        "src/test/resources/query/sql_function/sql_function_formatUDF/query03.sql", // date_format
                        "src/test/resources/query/sql_function/query33.sql" // timestamp accuracy
                ));
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.model.non-equi-join-recommendation-enabled", "TRUE");
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        MetadataTestUtils.updateProjectConfig(getProject(), "kylin.query.metadata.expose-computed-column", "FALSE");
        executeTestScenarioWithStorageType(storageType, new TestScenario(CompareLevel.SAME, "query/sql_function"), //
                new TestScenario(CompareLevel.SAME, "query/sql_function/sql_function_nullHandling"), //
                new TestScenario(CompareLevel.SAME, "query/sql_function/sql_function_formatUDF"), //
                new TestScenario(CompareLevel.SAME, "query/sql_function/sql_function_DateUDF"), //
                new TestScenario(CompareLevel.SAME, "query/sql_function/sql_function_OtherUDF"), //
                new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn/sql_computedcolumn_StringUDF"), //
                new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn/sql_computedcolumn_nullHandling"), //
                new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn/sql_computedcolumn_formatUDF"), //
                new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn/sql_computedcolumn_DateUDF"), //
                new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn/sql_computedcolumn_OtherUDF"), //
                new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn/sql_expression"), //
                new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn/sql_select_group_same_column") //
        );
        ExecAndCompExt.currentTestGlutenDisabledSqls.remove();
    }

    @Test
    @Ignore("For development")
    public void testTemp() throws Exception {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.calcite.extras-props.conformance", "DEFAULT");
        overwriteSystemProp("kylin.query.metadata.expose-computed-column", "FALSE");
        overwriteSystemProp("calcite.debug", "true");
        new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/temp").execute();
    }

    @Test
    public void testCorr() throws Exception {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.calcite.extras-props.conformance", "DEFAULT");
        overwriteSystemProp("kylin.query.metadata.expose-computed-column", "FALSE");

        new TestScenario(CompareLevel.SAME, "query/sql_corr").execute();
    }

    @Test
    public void testGroupingSetsWithoutSplitGroupingSets() throws Exception {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.calcite.extras-props.conformance", "DEFAULT");
        overwriteSystemProp("kylin.query.engine.split-group-sets-into-union", "FALSE");
        new TestScenario(CompareLevel.SAME, "query/sql_grouping").execute();
    }

    @Ignore("For development")
    @Test
    public void testQueryForPreparedMetadata() throws Exception {
        TestScenario scenario = new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/temp");
        collectQueries(Lists.newArrayList(scenario));
        List<Pair<String, String>> queries = scenario.getQueries();
        populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
        ExecAndCompExt.execAndCompare(queries, getProject(), scenario.getCompareLevel(),
                scenario.getJoinType().toString());
    }

    @Test
    public void testCCWithSelectStar() throws Exception {
        MetadataTestUtils.updateProjectConfig(getProject(), "kylin.query.metadata.expose-computed-column", "TRUE");

        String folder = "query/sql_computedcolumn/sql_computedcolumn_with_select_star";
        new TestScenario(CompareLevel.SAME, folder, 0, 1).execute();
        new TestScenario(CompareLevel.NONE, folder, 1, 4).execute();
    }

    @Test
    public void testEscapeParentheses() throws Exception {
        overwriteSystemProp("kylin.query.transformers",
                "org.apache.kylin.query.util.CognosParenthesesEscapeTransformer,org.apache.kylin.query.util.ConvertToComputedColumn, org.apache.kylin.query.util.DefaultQueryTransformer,org.apache.kylin.query.util.EscapeTransformer,org.apache.kylin.query.util.KeywordDefaultDirtyHack");
        overwriteSystemProp("kylin.query.pushdown.converter-class-names",
                "org.apache.kylin.query.util.CognosParenthesesEscapeTransformer,org.apache.kylin.query.util.RestoreFromComputedColumn,org.apache.kylin.query.util.SparkSQLFunctionConverter");
        overwriteSystemProp("kylin.query.table-detect-transformers",
                "org.apache.kylin.query.util.CognosParenthesesEscapeTransformer,org.apache.kylin.query.util.DefaultQueryTransformer,org.apache.kylin.query.util.EscapeTransformer");
        new TestScenario(CompareLevel.SAME, "query/sql_parentheses_escape").execute();
    }

    @Test
    public void testOrdinalQuery() throws Exception {
        overwriteSystemProp("kylin.query.calcite.extras-props.conformance", "LENIENT");
        new TestScenario(CompareLevel.SAME, "query/sql_ordinal").execute();
    }

    @Test
    public void testDynamicQuery() throws Exception {
        TestScenario testScenario = new TestScenario(CompareLevel.SAME, "query/sql_dynamic");
        testScenario.setDynamicSql(true);
        testScenario.execute();
    }

    @Test
    public void testCalciteOperatorTablesConfig() throws Exception {
        overwriteSystemProp("kylin.query.calcite.extras-props.FUN", "standard,oracle");
        executeTestScenarioWithStorageType(storageType,
                new TestScenario(CompareLevel.SAME, "query/sql_function/oracle_function"), // NVL
                new TestScenario(CompareLevel.SAME, "query/sql_function/sql_function_DateUDF") // make sure udfs are executed correctly
        );
    }

    /**
     * Following cased are not supported in auto-model test
     */
    @Ignore("not supported")
    @Test
    public void testNotSupported() throws Exception {

        // percentile and sql_intersect_count do not support
        // new TestScenario(CompareLevel.SAME, "sql_intersect_count")
        // new TestScenario(CompareLevel.SAME, "sql_percentile")//,

        /* CompareLevel = SAME */

        // Covered by manual test with fixed
        new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn").execute();
        new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn_common").execute();
        new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn_leftjoin").execute();

        /* CompareLevel = NONE */

        // test bad query detector
        // see ITKapKylinQueryTest.runTimeoutQueries
        new TestScenario(CompareLevel.NONE, "query/sql_timeout").execute();

        // stream not testable
        new TestScenario(CompareLevel.NONE, "query/sql_streaming").execute();

        // see ITMassInQueryTest
        new TestScenario(CompareLevel.NONE, "query/sql_massin_distinct").execute();
        new TestScenario(CompareLevel.NONE, "query/sql_massin").execute();
        new TestScenario(CompareLevel.NONE, "query/sql_intersect_count").execute();

        // see ITKylinQueryTest.testInvalidQuery
        new TestScenario(CompareLevel.NONE, "query/sql_invalid").execute();

        /* CompareLevel = SAME_ROWCOUNT */
    }

    @Test
    public void testConformance() throws Exception {
        overwriteSystemProp("kylin.query.calcite.extras-props.conformance", "LENIENT");
        new TestScenario(CompareLevel.SAME, "query/sql_conformance").execute();
    }

    @Test
    //reference KE-11887
    public void testQuerySingleValue() throws Exception {
        final String TEST_FOLDER = "query/sql_single_value";
        TestScenario testScenario = new TestScenario(CompareLevel.NONE, TEST_FOLDER, 0, 2);
        //query00 subquery return multi null row
        //query01 should throw RuntimeException
        try {
            testScenario.execute();
            Assert.fail();
        } catch (Exception e) {
            ConcurrentHashMap<String, Set<String>> failedQueries = testScenario.getFailedQueries();
            Assert.assertEquals(1, failedQueries.size());
            String rc = "more than 1 row returned in a single value aggregation";
            Assert.assertTrue(failedQueries.containsKey(rc));
            Assert.assertEquals(2, failedQueries.get(rc).size());
        }

        //query02 support SINGLE_VALUE
        //query03 support :Issue 4337 , select (select '2012-01-02') as data, xxx from table group by xxx
        //query04 support :Subquery is null
        new TestScenario(CompareLevel.SAME, TEST_FOLDER, 2, 5).execute();

    }

    @Test
    // reference KE-39108
    public void testAggPushToLeftJoin() throws Exception {
        new TestScenario(CompareLevel.SAME, "query/sql_agg_left_join").execute();
    }

    @Test
    public void testInRowOperator() throws Exception {
        // run gluten, after KE-44563 is fixed
        ExecAndCompExt.currentTestGlutenDisabledSqls.set(Sets.newHashSet(
                "src/test/resources/query/sql_in_row/query01.sql",
                "src/test/resources/query/sql_in_row/query02.sql"));
        
        new TestScenario(CompareLevel.SAME, "query/sql_in_row").execute();

        ExecAndCompExt.currentTestGlutenDisabledSqls.remove();
    }

}

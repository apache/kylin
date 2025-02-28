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

package org.apache.kylin.util;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.query.StructField;
import org.apache.kylin.query.KylinTestBase;
import org.apache.kylin.query.engine.data.QueryResult;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.util.Throwables;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExecAndCompExt extends ExecAndComp {

    static ForkJoinPool pool = new ForkJoinPool(2);

    public static ThreadLocal<Set<String>> currentTestGlutenDisabledSqls = new ThreadLocal<>();

    @SneakyThrows
    public static void execLimitAndValidateNew(SuggestTestBase.TestScenario scenario, String prj, String joinType,
            Map<String, CompareEntity> recAndQueryResult) {
        trackAndRunQueries(pool, scenario, query -> {
            log.info("execLimitAndValidate on query: {}", query.getFirst());
            String sql = changeJoinType(query.getSecond(), joinType);
            Pair<String, String> sqlAndAddedLimitSql = Pair.newPair(sql, sql);
            if (!sql.toLowerCase(Locale.ROOT).contains("limit ")) {
                sqlAndAddedLimitSql.setSecond(sql + " limit 5");
            }
            val modelResult = queryModelWithOlapContext(prj, joinType, sqlAndAddedLimitSql.getSecond());
            if (recAndQueryResult != null) {
                val entity = recAndQueryResult.computeIfAbsent(query.getSecond(), k -> new CompareEntity());
                entity.setSql(sqlAndAddedLimitSql.getSecond());
                entity.setOlapContexts(modelResult.olapContexts);
            }
            addQueryPath(recAndQueryResult, query, sql);
            val sparkResult = queryWithVanillaSpark(prj, sql, joinType, query.getFirst());
            if (!QueryResultComparator.compareResults(sparkResult, modelResult.getQueryResult(), CompareLevel.SUBSET)) {
                ExecAndComp.throwResultNotMatch(joinType, query);
            }
        });
    }

    @SneakyThrows
    public static void execAndCompareDynamic(SuggestTestBase.TestScenario scenario, String prj,
            CompareLevel compareLevel, String joinType, Map<String, CompareEntity> recAndQueryResult) {
        trackAndRunQueries(pool, scenario, path2Sql -> {
            try {
                log.info("Exec and compare query ({}) :{}", joinType, path2Sql.getFirst());
                String sql = changeJoinType(path2Sql.getSecond(), joinType);
                long startTime = System.currentTimeMillis();
                List<String> params = KylinTestBase.getParameterFromFile(new File(path2Sql.getFirst().trim()));

                val modelResult = queryModelWithOlapContext(prj, joinType, sql, params);

                val entity = recAndQueryResult.computeIfAbsent(path2Sql.getSecond(), k -> new CompareEntity());
                entity.setSql(path2Sql.getSecond());
                entity.setOlapContexts(modelResult.olapContexts);

                val sparkResult = queryWithVanillaSpark(prj, path2Sql.getSecond(), joinType, path2Sql.getFirst());

                if (!QueryResultComparator.compareResults(sparkResult, modelResult.getQueryResult(), compareLevel)) {
                    log.error("Failed on compare query ({}) :{}", joinType, sql);
                    ExecAndComp.throwResultNotMatch(joinType, path2Sql);
                }

                addQueryPath(recAndQueryResult, path2Sql, sql);
                printTimeCost(joinType, sql, startTime);
            } catch (IOException e) {
                throw new IllegalStateException("run with join type (" + joinType + "):\n" + path2Sql.getFirst());
            }
        });
    }

    private static void printTimeCost(String joinType, String sql, long startTime) {
        log.info("The query ({}) : {} cost {} (ms)", joinType, sql, System.currentTimeMillis() - startTime);
    }

    public static void execAndCompare(SuggestTestBase.TestScenario scenario, String prj, CompareLevel compareLevel,
            String joinType) {
        execAndCompare(scenario, prj, compareLevel, joinType, null, null);
    }

    @SneakyThrows
    public static void execAndCompare(SuggestTestBase.TestScenario scenario, String prj, CompareLevel compareLevel,
            String joinType, Map<String, CompareEntity> recAndQueryResult, Pair<String, String> views) {
        QueryContext.current().close();
        QueryContext.current().setProject(prj);
        trackAndRunQueries(pool, scenario, query -> {
            try (val ignored = QueryContext.current()) {
                log.info("Exec and compare query ({}) :{}", joinType, query.getFirst());
                String sql = changeJoinType(query.getSecond(), joinType);
                long startTime = System.currentTimeMillis();
                EnhancedQueryResult modelResult = queryModelWithOlapContext(prj, joinType, sql);
                if (recAndQueryResult != null) {
                    val entity = recAndQueryResult.computeIfAbsent(query.getSecond(), k -> new CompareEntity());
                    entity.setSql(sql);
                    entity.setOlapContexts(modelResult.olapContexts);
                }
                List<StructField> cubeColumns = modelResult.getColumns();
                addQueryPath(recAndQueryResult, query, sql);
                if (compareLevel != CompareLevel.NONE
                        && !scenario.getIgnoredPathComparingResult().contains(query.getFirst())) {
                    String newSql = sql;
                    if (views != null) {
                        newSql = sql.replaceAll(views.getFirst(), views.getSecond());
                    }
                    long startTs = System.currentTimeMillis();
                    val sparkResult = queryWithVanillaSpark(prj, newSql, joinType, query.getFirst());
                    if ((compareLevel == CompareLevel.SAME || compareLevel == CompareLevel.SAME_ORDER)
                            && sparkResult.getColumns().size() != cubeColumns.size()) {
                        log.error("Failed on compare query ({}) :{} \n cube schema: {} \n, spark schema: {}", joinType,
                                query, cubeColumns, sparkResult.getColumns());
                        throw new IllegalStateException("query (" + joinType + ") :" + query + " schema not match");
                    }
                    if (!inToDoList(query.getFirst()) && compareLevel == CompareLevel.SAME) {
                        QueryResultComparator.compareColumnType(cubeColumns, sparkResult.getColumns());
                    }

                    log.info("Query with Spark Duration(ms): {}", System.currentTimeMillis() - startTs);

                    startTs = System.currentTimeMillis();
                    if (!QueryResultComparator.compareResults(sparkResult, modelResult.getQueryResult(),
                            compareLevel)) {
                        ExecAndComp.throwResultNotMatch(joinType, query);
                    }
                    log.info("Compare Duration(ms): {}", System.currentTimeMillis() - startTs);
                } else {
                    log.info("result comparison is not available");
                }
                printTimeCost(joinType, query.getFirst(), startTime);
            }
        });
    }

    public static boolean execAndCompareQueryResult(Pair<String, String> queryForKap,
            Pair<String, String> queryForSpark, String joinType, String prj,
            Map<String, CompareEntity> recAndQueryResult) {
        String sqlForSpark = changeJoinType(queryForSpark.getSecond(), joinType);
        addQueryPath(recAndQueryResult, queryForSpark, sqlForSpark);
        QueryResult sparkResult = queryWithVanillaSpark(prj, queryForSpark.getSecond(), joinType,
                queryForSpark.getFirst());

        String sqlForKap = changeJoinType(queryForKap.getSecond(), joinType);
        val cubeResult = queryModelWithMassage(prj, sqlForKap, null);

        return sparkResult.getRows().equals(cubeResult.getRows());
    }

    public static void addQueryPath(Map<String, CompareEntity> recAndQueryResult, Pair<String, String> query,
            String modifiedSql) {
        if (recAndQueryResult == null) {
            return;
        }

        Preconditions.checkState(recAndQueryResult.containsKey(modifiedSql));
        recAndQueryResult.get(modifiedSql).setFilePath(query.getFirst());
    }

    @SneakyThrows
    public static void trackAndRunQueries(ForkJoinPool pool, SuggestTestBase.TestScenario secenario,
            Consumer<Pair<String, String>> action) {
        Set<String> glutenDisabledSqls = currentTestGlutenDisabledSqls.get();
        pool.submit(() -> {
            secenario.getQueries().parallelStream().forEach(query -> {
                try {
                    SparkSession ss = SparkSession.active();
                    if (CollectionUtils.isNotEmpty(glutenDisabledSqls) && ss != null
                            && glutenDisabledSqls.stream().anyMatch(query.getFirst()::endsWith)) {
                        log.info("Disable gluten on query: {}", query.getFirst());
                        ss.sparkContext().setLocalProperty("gluten.enabledForCurrentThread", "false");
                        action.accept(query);
                        ss.sparkContext().setLocalProperty("gluten.enabledForCurrentThread", null);
                        return;
                    }
                    action.accept(query);
                } catch (Exception e) {
                    log.error("Failed on query: {}", query.getFirst());
                    Throwable rootCause = Throwables.getRootCause(e);
                    String rcMsg = rootCause == null ? "NullPointerException" : rootCause.getMessage();
                    secenario.getFailedQueries().putIfAbsent(rcMsg, Sets.newConcurrentHashSet());
                    secenario.getFailedQueries().get(rcMsg).add(query.getFirst());
                }
            });
        }).get();
    }

    @Getter
    @Setter
    public static class CompareEntity {

        private String sql;
        @ToString.Exclude
        private Collection<OlapContext> olapContexts;
        @ToString.Exclude
        private AccelerateInfo accelerateInfo;
        private String accelerateLayouts;
        private String queryUsedLayouts;
        private RecAndQueryCompareUtil.AccelerationMatchedLevel level;
        private String filePath;

        @Override
        public String toString() {
            return "CompareEntity{\n\tsql=[" + QueryUtil.removeCommentInSql(sql) + "],\n\taccelerateLayouts="
                    + accelerateLayouts + ",\n\tqueryUsedLayouts=" + queryUsedLayouts + "\n\tfilePath=" + filePath
                    + ",\n\tlevel=" + level + "\n}";
        }

        public boolean ignoredCompareLevel() {
            return getLevel() == RecAndQueryCompareUtil.AccelerationMatchedLevel.SNAPSHOT_QUERY
                    || getLevel() == RecAndQueryCompareUtil.AccelerationMatchedLevel.SIMPLE_QUERY
                    || getLevel() == RecAndQueryCompareUtil.AccelerationMatchedLevel.CONSTANT_QUERY;
        }
    }
}

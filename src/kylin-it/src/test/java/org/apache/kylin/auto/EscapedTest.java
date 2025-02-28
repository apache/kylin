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

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.util.ExecAndComp;
import org.apache.kylin.util.ExecAndCompExt;
import org.apache.kylin.util.QueryResultComparator;
import org.apache.kylin.util.SuggestTestBase;
import org.apache.spark.sql.SparderEnv;
import org.junit.Test;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EscapedTest extends SuggestTestBase {
    @Test
    public void testSimilarTo() throws Exception {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        executeTestScenario(BuildAndCompareContext.builder().expectModelNum(2)
                .testScenarios(Lists.newArrayList(new TestScenario(ExecAndComp.CompareLevel.SAME, "query/sql_escaped")))
                .build());
    }

    protected void buildAndCompare(TestScenario... testScenarios) throws Exception {
        try {
            // 2. execute cube building
            long startTime = System.currentTimeMillis();
            buildAllModels(kylinConfig, getProject());
            log.debug("build cube cost {} ms", System.currentTimeMillis() - startTime);

            // dump metadata for debugging
            dumpMetadata();

            // 3. validate results between SparkSQL and cube
            startTime = System.currentTimeMillis();
            populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
            Arrays.stream(testScenarios).forEach(testScenario -> {
                populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
                execAndCompareEscaped(testScenario.getQueries(), getProject(), testScenario.getCompareLevel(),
                        testScenario.getJoinType().name());
            });
            log.debug("compare result cost {} s", System.currentTimeMillis() - startTime);
        } finally {
            FileUtils.deleteQuietly(new File("../kylin-it/metastore_db"));
        }
    }

    public void execAndCompareEscaped(List<Pair<String, String>> queries, String prj,
            ExecAndComp.CompareLevel compareLevel, String joinType) {
        for (Pair<String, String> query : queries) {
            log.info("Exec and compare query ({}) :{}", joinType, query.getFirst());
            String sql = ExecAndComp.changeJoinType(query.getSecond(), joinType);

            // query not escaped sql from cube and spark
            long startTime = System.currentTimeMillis();
            val cubeResult = ExecAndComp.queryModelWithOlapContext(prj, joinType, sql);
            ExecAndCompExt.addQueryPath(null, query, sql);
            val sparkResult = ExecAndComp.queryWithVanillaSpark(prj, sql, query.getFirst(), query.getFirst());

            // make ke not escape sql and escape sql manually
            overwriteSystemProp("kylin.query.parser.escaped-string-literals", "true");
            val eSql = sql.replace("\\\\", "\\");

            // query escaped sql from cube and spark
            val cubeResult2 = ExecAndComp.queryModelWithOlapContext(prj, joinType, eSql);
            SparderEnv.getSparkSession().conf().set("spark.sql.parser.escapedStringLiterals", true);
            val sparkResult2 = ExecAndComp.queryWithVanillaSpark(prj, eSql, query.getFirst(), query.getFirst());
            if ((compareLevel == ExecAndComp.CompareLevel.SAME || compareLevel == ExecAndComp.CompareLevel.SAME_ORDER)
                    && sparkResult.getColumns().size() != cubeResult.getColumns().size()) {
                log.error("Failed on compare query ({}) :{} \n cube schema: {} \n, spark schema: {}", joinType, query,
                        cubeResult.getColumns(), sparkResult.getColumns());
                throw new IllegalStateException("query (" + joinType + ") :" + query + " schema not match");
            }

            // compare all result set
            if (!QueryResultComparator.compareResults(sparkResult, cubeResult.getQueryResult(), compareLevel)
                    || !QueryResultComparator.compareResults(sparkResult, sparkResult2, compareLevel)
                    || !QueryResultComparator.compareResults(sparkResult2, cubeResult2.getQueryResult(),
                            compareLevel)) {
                log.error("Failed on compare query ({}) :{}", joinType, query);
                ExecAndComp.throwResultNotMatch(joinType, query);
            }
            log.info("The query ({}) : {} cost {} (ms)", joinType, query, System.currentTimeMillis() - startTime);

            // restore env
            restoreSystemProp("kylin.query.parser.escaped-string-literals");
            SparderEnv.getSparkSession().conf().unset("spark.sql.parser.escapedStringLiterals");
        }
    }
}

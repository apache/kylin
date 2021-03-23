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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.Quadruple;
import org.apache.kylin.engine.spark2.utils.QueryUtil;
import org.apache.kylin.engine.spark2.utils.RecAndQueryCompareUtil.CompareEntity;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.KylinSparkEnv;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.common.SparkQueryTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class NExecAndComp {
    private static final Logger logger = LoggerFactory.getLogger(NExecAndComp.class);

    public enum CompareLevel {
        SAME, // exec and compare
        SAME_ORDER, // exec and compare order
        SAME_ROWCOUNT,
        SUBSET,
        NONE, // Do not compare and just return OK
        // Generate the compared metrics when adding new sqls
        // Note: must add ';' in the end of new sql
        GEN_METRICS,
        // Generate the compared results and metrics when adding new sqls
        // Note: must add ';' in the end of new sql, and the results will be saved into csv file
        GEN_RESULTS,
    }

    private static ObjectMapper objectMapper = new ObjectMapper();

    static void execLimitAndValidate(List<Pair<String, String>> queries, String prj, String joinType) {
        execLimitAndValidateNew(queries, prj, joinType, null);
    }

    public static void execLimitAndValidateNew(List<Pair<String, String>> queries, String prj, String joinType,
                                               Map<String, CompareEntity> recAndQueryResult) {

        int appendLimitQueries = 0;
        for (Pair<String, String> query : queries) {
            logger.info("execLimitAndValidate on query: " + query.getFirst());
            String sql = changeJoinType(query.getSecond(), joinType);

            Pair<String, String> sqlAndAddedLimitSql = Pair.newPair(sql, sql);
            if (!sql.toLowerCase(Locale.ROOT).contains("limit ")) {
                sqlAndAddedLimitSql.setSecond(sql + " limit 5");
                appendLimitQueries++;
            }

            Pair<Dataset<Row>, ITQueryMetrics> queryResult =
                    (recAndQueryResult == null) ? queryWithKylin(prj, joinType,
                            sqlAndAddedLimitSql, null)
                    : queryWithKylin(prj, joinType, sqlAndAddedLimitSql, null, recAndQueryResult);
            Dataset<Row> kylinResult = queryResult.getFirst();
            addQueryPath(recAndQueryResult, query, sql);
            Dataset<Row> sparkResult = queryWithSpark(prj, sql, query.getFirst());
            List<Row> kylinRows = SparkQueryTest.castDataType(kylinResult, sparkResult).toJavaRDD().collect();
            List<Row> sparkRows = sparkResult.toJavaRDD().collect();
            if (compareResults(normRows(sparkRows), normRows(kylinRows), CompareLevel.SUBSET)) {
                throw new IllegalArgumentException("Result not match");
            }
        }
        logger.info("Queries appended with limit: " + appendLimitQueries);
    }

    public static void execAndCompare(List<Pair<String, String>> queries, String prj, CompareLevel compareLevel,
                                      String joinType) {
        execAndCompareNew(queries, prj, compareLevel, joinType, null);
    }

    public static void execAndCompareQueryList(List<String> queries, String prj, CompareLevel compareLevel,
                                               String joinType) {
        List<Pair<String, String>> transformed = queries.stream().map(q -> Pair.newPair("", q))
                .collect(Collectors.toList());
        execAndCompareNew(transformed, prj, compareLevel, joinType, null);
    }

    public static void execAndCompareNew(List<Pair<String, String>> queries, String prj, CompareLevel compareLevel,
                                         String joinType, Map<String, CompareEntity> recAndQueryResult) {
        for (Pair<String, String> query : queries) {
            // init QueryContext
            QueryContextFacade.resetCurrent();
            logger.info("Exec and compare query ({}) :{}", joinType, query.getFirst());

            String sql = changeJoinType(query.getSecond(), joinType);

            // Query from Cube
            long startTime = System.currentTimeMillis();
            Pair<Dataset<Row>, ITQueryMetrics> queryResult =
                    (recAndQueryResult == null) ? queryWithKylin(prj, joinType,
                            Pair.newPair(sql, sql), null)
                    : queryWithKylin(prj, joinType, Pair.newPair(sql, sql), null,
                            recAndQueryResult);
            addQueryPath(recAndQueryResult, query, sql);
            Dataset<Row> cubeResult = queryResult.getFirst();
            if (compareLevel == CompareLevel.SAME) {
                Dataset<Row> sparkResult = null;
                String csvDataPathStr = query.getFirst() + ".expected";
                if(new File(csvDataPathStr).exists()) {
                    logger.debug("Use expected dataset for {}", sql);
                    sparkResult = KylinSparkEnv.getSparkSession().read().csv(csvDataPathStr);
                } else {
                    sparkResult = queryWithSpark(prj, sql, query.getFirst());
                }
                String result = SparkQueryTest.checkAnswer(SparkQueryTest.castDataType(cubeResult, sparkResult), sparkResult, false);
                if (result != null) {
                    logger.error("Failed on compare query ({}) :{}", joinType, query);
                    logger.error(result);
                    throw new IllegalArgumentException("query (" + joinType + ") :" + query + " result not match");
                } else {
                    logger.debug("Passed {}", query.getFirst());
                }
            } else if (compareLevel == CompareLevel.NONE) {
                Dataset<Row> sparkResult = queryWithSpark(prj, sql, query.getFirst());
                List<Row> sparkRows = sparkResult.toJavaRDD().collect();
                List<Row> kylinRows = SparkQueryTest.castDataType(cubeResult, sparkResult).toJavaRDD().collect();
                if (compareResults(normRows(sparkRows), normRows(kylinRows), compareLevel)) {
                    logger.error("Failed on compare query ({}) :{}", joinType, query);
                    throw new IllegalArgumentException("query (" + joinType + ") :" + query + " result not match");
                }
            } else {
                cubeResult.persist();
                logger.info(
                        "result comparision is not available for {}, part of the cube results: {},\n {}" , query.getFirst(),
                        cubeResult.count(), cubeResult.showString(20, 25 , false));
                cubeResult.unpersist();
            }
            logger.trace("The query ({}) : {} cost {} (ms)", query.getFirst(), "", System.currentTimeMillis() - startTime);
        }
    }

    public static void execAndCompareNew2(List<Quadruple<String, String, ITQueryMetrics, List<String>>> queries,
                                          String prj, CompareLevel compareLevel, String joinType,
                                          Map<String, CompareEntity> recAndQueryResult,
                                          String sqlFolder) throws IOException{
        for (Quadruple<String, String, ITQueryMetrics, List<String>> query : queries) {
            // init QueryContext
            QueryContextFacade.resetCurrent();
            logger.info("Exec and compare query ({}) :{}", joinType, query.getFirst());

            String sql = changeJoinType(query.getSecond(), joinType);
            // Query from Cube
            long startTime = System.currentTimeMillis();
            Pair<Dataset<Row>, ITQueryMetrics> queryResult =
                    (recAndQueryResult == null) ? queryWithKylin(prj, joinType,
                            Pair.newPair(sql, sql), query.getFourth())
                    : queryWithKylin(prj, joinType, Pair.newPair(sql, sql), query.getFourth(),
                            recAndQueryResult);
            ITQueryMetrics collectedMetrics = queryResult.getSecond();

            if (compareLevel == CompareLevel.GEN_METRICS) {
                // generate metrics
                try {
                    File newSqlFile = new File(query.getFirst());
                    FileUtils.writeStringToFile(newSqlFile,
                            query.getSecond().trim() + "\n;", Charsets.UTF_8, false);
                    String json = objectMapper.writeValueAsString(collectedMetrics);
                    FileUtils.writeStringToFile(newSqlFile,
                            json, Charsets.UTF_8, true);
                } catch (JsonProcessingException e) {
                    logger.error("Write metrics values as string error: ", e);
                }
            } else if (compareLevel == CompareLevel.GEN_RESULTS) {
                // generate metrics
                try {
                    File newSqlFile = new File(query.getFirst());
                    FileUtils.writeStringToFile(newSqlFile,
                            query.getSecond().trim() + "\n;", Charsets.UTF_8, false);
                    String json = objectMapper.writeValueAsString(collectedMetrics);
                    FileUtils.writeStringToFile(newSqlFile,
                            json, Charsets.UTF_8, true);
                } catch (JsonProcessingException e) {
                    logger.error("Write metrics values as string error: ", e);
                }
                // generate results and save them into csv file
                try {
                    queryResult.getFirst().repartition(1).write()
                            .option("header", false)
                            .option("nullValue", "\"-\"")
                            .csv(genResultsFiles(query.getFirst()));
                } catch (JsonProcessingException e) {
                    logger.error("Write results as csv file error: ", e);
                }
            } else {
                if(!checkQueryMetrics(query.getThird(), collectedMetrics)) {
                    logger.error("Query metrics not match, excepted: {}, results: {} ! Please check " +
                                    "SQL: {} in {}", query.getThird(), collectedMetrics, sql,
                            query.getFirst());
                    throw new IllegalArgumentException("Query metrics not match!");
                }
                if (sqlFolder.equalsIgnoreCase("sql_exactly_agg")) {
                    if (!query.getThird().getExactlyMatched()
                            .equals(collectedMetrics.getExactlyMatched())) {
                        logger.error("Query metrics not match, excepted: {}, results: {} ! Please check " +
                                        "SQL: {} in {}", query.getThird(), collectedMetrics, sql,
                                query.getFirst());
                        throw new IllegalArgumentException("Query metrics not match!");
                    }
                }
            }

            Dataset<Row> cubeResult = queryResult.getFirst();
            addQueryPath2(recAndQueryResult, query.getFirst(), sql);
            if (compareLevel == CompareLevel.SAME) {
                Dataset<Row> sparkResult = null;
                String csvDataPathStr = query.getFirst() + ".expected";
                if(new File(csvDataPathStr).exists()) {
                    logger.debug("Use expected dataset for {}", sql);
                    sparkResult = KylinSparkEnv.getSparkSession().read()
                            .option("nullValue", "\"-\"").csv(csvDataPathStr);
                } else {
                    sparkResult = queryWithSpark(prj, sql, query.getFirst(), query.getFourth());
                }
                String result = SparkQueryTest.checkAnswer(SparkQueryTest.castDataType(cubeResult, sparkResult), sparkResult, false);
                if (result != null) {
                    logger.error("Failed on compare query ({}) :{}", joinType, query);
                    logger.error(result);
                    throw new IllegalArgumentException("query (" + joinType + ") :" + query + " result not match");
                } else {
                    logger.debug("Passed {}", query.getFirst());
                }
            } else if (compareLevel == CompareLevel.SAME_ROWCOUNT
                    || compareLevel == CompareLevel.SAME_ORDER
                    || compareLevel == CompareLevel.SUBSET) {
                Dataset<Row> sparkResult = queryWithSpark(prj, sql, query.getFirst(), query.getFourth());
                List<Row> sparkRows = sparkResult.toJavaRDD().collect();
                List<Row> kylinRows = SparkQueryTest.castDataType(cubeResult, sparkResult).toJavaRDD().collect();
                if (compareResults(normRows(sparkRows), normRows(kylinRows), compareLevel)) {
                    logger.error("Failed on compare query ({}) :{}", joinType, query);
                    throw new IllegalArgumentException("query (" + joinType + ") :" + query + " result not match");
                }
            } else {
                cubeResult.persist();
                logger.info(
                        "result comparision is not available for {}, part of the cube results: {},\n {}" , query.getFirst(),
                        cubeResult.count(), cubeResult.showString(20, 25 , false));
                cubeResult.unpersist();
            }
            logger.trace("The query ({}) : {} cost {} (ms)", query.getFirst(), "", System.currentTimeMillis() - startTime);
        }
    }

    public static boolean checkQueryMetrics(ITQueryMetrics comparedMetrics, ITQueryMetrics collectedMetrics) {
        return comparedMetrics.equals(collectedMetrics);
    }

    public static boolean execAndCompareQueryResult(Pair<String, String> queryForKylin,
                                                    Pair<String, String> queryForSpark, String joinType, String prj,
                                                    Map<String, CompareEntity> recAndQueryResult) {
        String sqlForSpark = changeJoinType(queryForSpark.getSecond(), joinType);
        addQueryPath(recAndQueryResult, queryForSpark, sqlForSpark);
        Dataset<Row> sparkResult = queryWithSpark(prj, queryForSpark.getSecond(), queryForSpark.getFirst());
        List<Row> sparkRows = sparkResult.toJavaRDD().collect();

        String sqlForKylin = changeJoinType(queryForKylin.getSecond(), joinType);
        Pair<Dataset<Row>, ITQueryMetrics> pair = queryWithKylin(prj, joinType,
                Pair.newPair(sqlForKylin, sqlForKylin), null);
        List<Row> kylinRows = SparkQueryTest.castDataType(pair.getFirst(), sparkResult).toJavaRDD().collect();

        return sparkRows.equals(kylinRows);
    }

    private static List<Row> normRows(List<Row> rows) {
        List<Row> rowList = Lists.newArrayList();
        rows.forEach(row -> {
            rowList.add(SparkQueryTest.prepareRow(row));
        });
        return rowList;
    }

    private static void addQueryPath(Map<String, CompareEntity> recAndQueryResult, Pair<String, String> query,
                                     String modifiedSql) {
        if (recAndQueryResult == null) {
            return;
        }

        Preconditions.checkState(recAndQueryResult.containsKey(modifiedSql));
        recAndQueryResult.get(modifiedSql).setFilePath(query.getFirst());
    }

    private static void addQueryPath2(Map<String, CompareEntity> recAndQueryResult, String filePath,
                                     String modifiedSql) {
        if (recAndQueryResult == null) {
            return;
        }

        Preconditions.checkState(recAndQueryResult.containsKey(modifiedSql));
        recAndQueryResult.get(modifiedSql).setFilePath(filePath);
    }

    @Deprecated
    static void execCompareQueryAndCompare(List<Pair<String, String>> queries, String prj, String joinType) {
        throw new IllegalStateException(
                "The method has deprecated, please call org.apache.kylin.engine.spark2.NExecAndComp.execAndCompareNew");
    }

    private static Pair<Dataset<Row>, ITQueryMetrics> queryWithKylin(String prj, String joinType, Pair<String, String> pair,
                                               List<String> parameters, Map<String, CompareEntity> compareEntityMap) {

        compareEntityMap.putIfAbsent(pair.getFirst(), new CompareEntity());
        final CompareEntity entity = compareEntityMap.get(pair.getFirst());
        entity.setSql(pair.getFirst());
        Pair<Dataset<Row>, ITQueryMetrics> queryResult = queryFromCube(prj,
                changeJoinType(pair.getSecond(), joinType), parameters);
        entity.setOlapContexts(OLAPContext.getThreadLocalContexts());
        OLAPContext.clearThreadLocalContexts();
        return queryResult;
    }

    private static Pair<Dataset<Row>, ITQueryMetrics> queryWithKylin(String prj, String joinType,
                                                                     Pair<String, String> sql, List<String> parameters) {
        return queryFromCube(prj, changeJoinType(sql.getSecond(), joinType), parameters);
    }

    private static Dataset<Row> queryWithSpark(String prj, String originSql, String sqlPath) {
        return queryWithSpark(prj, originSql, sqlPath, null);
    }

    private static Dataset<Row> queryWithSpark(String prj, String originSql, String sqlPath,
                List<String> parameters) {
        String compareSql = getCompareSql(sqlPath);
        if (StringUtils.isEmpty(compareSql))
            compareSql = originSql;

        String afterConvert = QueryUtil.massagePushDownSql(compareSql, prj, "default", false);
        // Table schema comes from csv and DATABASE.TABLE is not supported.
        String sqlForSpark = removeDataBaseInSql(afterConvert);
        if (parameters != null) {
            for (String param : parameters) {
                sqlForSpark = sqlForSpark.replaceFirst("\\?", "'" + param + "'");
            }
        }
        return querySparkSql(sqlForSpark);
    }

    public static String removeDataBaseInSql(String originSql) {
        return originSql.replaceAll("(?i)edw\\.", "") //
                .replaceAll("`edw`\\.", "") //
                .replaceAll("\"EDW\"\\.", "") //
                .replaceAll("`EDW`\\.", "") //
                .replaceAll("(?i)default\\.", "") //
                .replaceAll("`default`\\.", "") //
                .replaceAll("\"DEFAULT\"\\.", "") //
                .replaceAll("`DEFAULT`\\.", "") //
                .replaceAll("(?i)TPCH\\.", "") //
                .replaceAll("`TPCH`\\.", "") //
                .replaceAll("`tpch`\\.", "") //
                .replaceAll("(?i)TDVT\\.", "") //
                .replaceAll("\"TDVT\"\\.", "") //
                .replaceAll("`TDVT`\\.", "") //
                .replaceAll("\"POPHEALTH_ANALYTICS\"\\.", "") //
                .replaceAll("`POPHEALTH_ANALYTICS`\\.", "") //
                .replaceAll("(?i)ISSUES\\.", "")
                .replaceAll("SSB\\.", "")
                .replaceAll("\"SSB\"\\.", "")
                .replaceAll("`SSB`\\.", "");
    }

    public static List<Pair<String, String>> fetchQueries(String folder) throws IOException {
        File sqlFolder = new File(folder);
        return retrieveITSqls(sqlFolder);
    }

    public static List<Quadruple<String, String, ITQueryMetrics, List<String>>> fetchQueries2(String folder) throws IOException {
        File sqlFolder = new File(folder);
        return retrieveITSqls2(sqlFolder);
    }

    public static List<Pair<String, String>> fetchPartialQueries(String folder, int start, int end) throws IOException {
        File sqlFolder = new File(folder);
        List<Pair<String, String>> originalSqls = retrieveITSqls(sqlFolder);
        if (end > originalSqls.size()) {
            end = originalSqls.size();
        }
        return originalSqls.subList(start, end);
    }

    @SuppressWarnings("unused")
    private static List<Pair<String, String>> retrieveAllQueries(String baseDir) throws IOException {
        File[] sqlFiles = new File[0];
        if (baseDir != null) {
            File sqlDirF = new File(baseDir);
            if (sqlDirF.exists() && sqlDirF.listFiles() != null) {
                sqlFiles = new File(baseDir).listFiles((dir, name) -> name.startsWith("sql_"));
            }
        }
        List<Pair<String, String>> allSqls = new ArrayList<>();
        for (File file : Objects.requireNonNull(sqlFiles)) {
            allSqls.addAll(retrieveITSqls(file));
        }
        return allSqls;
    }

    private static List<Pair<String, String>> retrieveITSqls(File file) throws IOException {
        File[] sqlFiles = new File[0];
        if (file != null && file.exists() && file.listFiles() != null) {
            sqlFiles = file.listFiles((dir, name) -> name.endsWith(".sql"));
        }
        List<Pair<String, String>> ret = Lists.newArrayList();
        assert sqlFiles != null;
        Arrays.sort(sqlFiles, (o1, o2) -> {
            final String idxStr1 = o1.getName().replaceAll("\\D", "");
            final String idxStr2 = o2.getName().replaceAll("\\D", "");
            if (idxStr1.isEmpty() || idxStr2.isEmpty()) {
                return String.CASE_INSENSITIVE_ORDER.compare(o1.getName(), o2.getName());
            }
            return Integer.parseInt(idxStr1) - Integer.parseInt(idxStr2);
        });
        for (File sqlFile : sqlFiles) {
            String sqlStatement = FileUtils.readFileToString(sqlFile, "UTF-8").trim();
            int semicolonIndex = sqlStatement.lastIndexOf(";");
            String sql = semicolonIndex == sqlStatement.length() - 1 ? sqlStatement.substring(0, semicolonIndex)
                    : sqlStatement;
            ret.add(Pair.newPair(sqlFile.getCanonicalPath(), sql + '\n'));
        }
        return ret;
    }

    private static List<Quadruple<String, String, ITQueryMetrics, List<String>>> retrieveITSqls2(File file) throws IOException {
        File[] sqlFiles = new File[0];
        if (file != null && file.exists() && file.listFiles() != null) {
            sqlFiles = file.listFiles((dir, name) -> name.endsWith(".sql"));
        }
        List<Quadruple<String, String, ITQueryMetrics, List<String>>> ret = Lists.newArrayList();
        assert sqlFiles != null;
        Arrays.sort(sqlFiles, (o1, o2) -> {
            final String idxStr1 = o1.getName().replaceAll("\\D", "");
            final String idxStr2 = o2.getName().replaceAll("\\D", "");
            if (idxStr1.isEmpty() || idxStr2.isEmpty()) {
                return String.CASE_INSENSITIVE_ORDER.compare(o1.getName(), o2.getName());
            }
            return Integer.parseInt(idxStr1) - Integer.parseInt(idxStr2);
        });
        for (File sqlFile : sqlFiles) {
            String sqlStatement = FileUtils.readFileToString(sqlFile, "UTF-8").trim();
            int semicolonIndex = sqlStatement.lastIndexOf(";");
            String sql = null;
            ITQueryMetrics metrics = null;
            if (semicolonIndex == -1) {
                sql = sqlStatement;
                metrics = new ITQueryMetrics();
            } else {
                sql = sqlStatement.substring(0, semicolonIndex);
                if (semicolonIndex == (sqlStatement.length() - 1)) {
                    metrics = new ITQueryMetrics();
                } else {
                    String metricsStr = sqlStatement.substring(semicolonIndex + 1);
                    metrics = convertFromJson(metricsStr);
                }
            }
            List<String> parameters = getParameterFromFile(sqlFile);
            ret.add(Quadruple.create(sqlFile.getCanonicalPath(), sql + '\n', metrics, parameters));
        }
        return ret;
    }

    private static ITQueryMetrics convertFromJson(String metricsStr) throws IOException{
         final ObjectMapper objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
         ITQueryMetrics metrics = objectMapper.readValue(metricsStr, ITQueryMetrics.class);
         return metrics;
    }

    public static List<String> getParameterFromFile(File sqlFile) throws IOException {
        String sqlFileName = sqlFile.getAbsolutePath();
        int prefixIndex = sqlFileName.lastIndexOf(".sql");
        String dataFielName = sqlFileName.substring(0, prefixIndex) + ".dat";
        File dataFile = new File(dataFielName);
        List<String> parameters = null;
        if (dataFile.exists()) {
            parameters = Files.readLines(dataFile, Charset.defaultCharset());
        }
        return parameters;
    }

    public static String genResultsFiles(String sqlFileName) throws IOException {
        String resultsFielName = sqlFileName + ".expected";
        File resultsFile = new File(resultsFielName);
        if (resultsFile.exists()) {
            FileUtils.deleteDirectory(resultsFile);
        }
        return resultsFielName;
    }

    private static boolean compareResults(List<Row> expectedResult, List<Row> actualResult, CompareLevel compareLevel) {
        boolean good = true;
        if (compareLevel == CompareLevel.SAME_ORDER) {
            good = expectedResult.equals(actualResult);
        }
        if (compareLevel == CompareLevel.SAME) {
            if (expectedResult.size() == actualResult.size()) {
                if (expectedResult.size() > 15000) {
                    throw new RuntimeException(
                            "please modify the sql to control the result size that less than 15000 and it has "
                                    + actualResult.size() + " rows");
                }
                for (Row eRow : expectedResult) {
                    if (!actualResult.contains(eRow)) {
                        good = false;
                        break;
                    }
                }
            } else {
                good = false;
            }
        }

        if (compareLevel == CompareLevel.SAME_ROWCOUNT) {
            long count1 = expectedResult.size();
            long count2 = actualResult.size();
            good = count1 == count2;
        }

        if (compareLevel == CompareLevel.SUBSET) {
            for (Row eRow : actualResult) {
                if (!expectedResult.contains(eRow)) {
                    good = false;
                    break;
                }
            }
        }

        if (!good) {
            logger.error("Result not match");
            printRows("expected", expectedResult);
            printRows("actual", actualResult);
        }
        return !good;
    }

    private static void printRows(String source, List<Row> rows) {
        System.out.println("***********" + source + " start**********");
        rows.forEach(row -> System.out.println(row.mkString(" | ")));
        System.out.println("***********" + source + " end**********");
    }

    private static void compareResults(Dataset<Row> expectedResult, Dataset<Row> actualResult,
                                       CompareLevel compareLevel) {
        Preconditions.checkArgument(expectedResult != null);
        Preconditions.checkArgument(actualResult != null);

        try {
            expectedResult.persist();
            actualResult.persist();

            boolean good = true;

            if (compareLevel == CompareLevel.SAME) {
                long count1 = expectedResult.except(actualResult).count();
                long count2 = actualResult.except(expectedResult).count();
                if (count1 != 0 || count2 != 0) {
                    good = false;
                }
            }

            if (compareLevel == CompareLevel.SAME_ROWCOUNT) {
                long count1 = expectedResult.count();
                long count2 = actualResult.count();
                good = count1 == count2;
            }

            if (compareLevel == CompareLevel.SUBSET) {
                long count1 = actualResult.except(expectedResult).count();
                good = count1 == 0;
            }

            if (!good) {
                logger.error("Result not match");
                expectedResult.show(10000);
                actualResult.show(10000);
                throw new IllegalStateException();
            }
        } finally {
            expectedResult.unpersist();
            actualResult.unpersist();
        }
    }

    public static List<Pair<String, String>> doFilter(List<Pair<String, String>> sources,
                                                      final Set<String> exclusionList) {
        Preconditions.checkArgument(sources != null);
        Set<String> excludes = Sets.newHashSet(exclusionList);
        return sources.stream().filter(pair -> {
            final String[] splits = pair.getFirst().split(File.separator);
            return !excludes.contains(splits[splits.length - 1]);
        }).collect(Collectors.toList());
    }

    public static Pair<Dataset<Row>, ITQueryMetrics> queryFromCube(String prj, String sqlText, List<String> parameters) {
        sqlText = QueryUtil.massageSql(sqlText, prj, 0, 0, "DEFAULT");
        return sql(prj, sqlText, parameters);
    }

    public static Dataset<Row> querySparkSql(String sqlText) {
        logger.trace("Fallback this sql to original engine...");
        long startTs = System.currentTimeMillis();
        Dataset<Row> r = KylinSparkEnv.getSparkSession().sql(sqlText);
        logger.trace("Duration(ms): {}", (System.currentTimeMillis() - startTs));
        return r;
    }

    public static Pair<Dataset<Row>, ITQueryMetrics> sql(String prj, String sqlText) {
        return sql(prj, sqlText, null);
    }

    public static Pair<Dataset<Row>, ITQueryMetrics> sql(String prj, String sqlText, List<String> parameters) {
        if (sqlText == null)
            throw new RuntimeException("Sorry your SQL is null...");

        try {
            logger.trace("Try to query from cube....");
            long startTs = System.currentTimeMillis();
            Pair<Dataset<Row>, ITQueryMetrics> pair = queryCubeAndSkipCompute(prj, sqlText, parameters);
            logger.trace("Cool! This sql hits cube...");
            logger.trace("Duration(ms): {}", (System.currentTimeMillis() - startTs));
            return pair;
        } catch (Throwable e) {
            logger.error("There is no cube can be used for query [{}]", sqlText);
            logger.error("Reasons:", e);
            throw new RuntimeException("Error in running query [ " + sqlText.trim() + " ]", e);
        }
    }

    static Pair<Dataset<Row>, ITQueryMetrics> queryCubeAndSkipCompute(String prj, String sql, List<String> parameters) throws Exception {
        KylinSparkEnv.skipCompute();
        Pair<Dataset<Row>, ITQueryMetrics> pair = queryCube(prj, sql, parameters);
        return pair;
    }

    static Pair<Dataset<Row>, ITQueryMetrics> queryCubeAndSkipCompute(String prj, String sql) throws Exception {
        KylinSparkEnv.skipCompute();
        Pair<Dataset<Row>, ITQueryMetrics> pair = queryCube(prj, sql, null);
        return pair;
    }

    public static Pair<Dataset<Row>, ITQueryMetrics> queryCube(String prj, String sql) throws SQLException {
        return queryCube(prj, sql, null);
    }

    public static Pair<Dataset<Row>, ITQueryMetrics> queryCube(String prj, String sql, List<String> parameters) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        ITQueryMetrics metrics = null;
        try {
            conn = QueryConnection.getConnection(prj);
            stmt = conn.prepareStatement(sql);
            for (int i = 1; parameters != null && i <= parameters.size(); i++) {
                stmt.setString(i, parameters.get(i - 1).trim());
            }
            rs = stmt.executeQuery();
            metrics = collectQueryMetrics(prj, rs, sql);
        } finally {
            DBUtils.closeQuietly(rs);
            DBUtils.closeQuietly(stmt);
            DBUtils.closeQuietly(conn);
            //KylinSparkEnv.cleanCompute();
        }
        return new Pair<>((Dataset<Row>) QueryContextFacade.current().getDataset(), metrics);
    }

    private static ITQueryMetrics collectQueryMetrics(String project, ResultSet resultSet,
                                                      String sql) throws SQLException {
        Pair<List<List<String>>, List<SelectedColumnMeta>> r = createResponseFromResultSet(resultSet);
        SQLResponse response = buildSqlResponse(project, false, r.getFirst(), r.getSecond());
        long scanRowCount = response.getTotalScanCount();
        long scanFiles = response.getTotalScanFiles();
        long scanBytes = response.getTotalScanBytes();
        List<Long> hitCuboids = new ArrayList<>();
        List<Boolean> exactlyMatcheds = new ArrayList<>();
        Collection<OLAPContext> olapContexts = OLAPContext.getThreadLocalContexts();
        if (olapContexts != null) {
            Iterator<OLAPContext> olapContextIterator = olapContexts.iterator();
            while (olapContextIterator.hasNext()) {
                OLAPContext olapContext = olapContextIterator.next();
                if (olapContext.storageContext.getCuboid() != null) {
                    hitCuboids.add(olapContext.storageContext.getCuboid().getId());
                }
                exactlyMatcheds.add(olapContext.isExactlyAggregate);
            }
        }
        if (hitCuboids.size() == 0) {
            hitCuboids.add(-1L);
            logger.warn("Query: ({}) not hit cuboid!", sql);
        }
        return new ITQueryMetrics(scanRowCount, scanBytes, scanFiles, hitCuboids, exactlyMatcheds);
    }

    private static Pair<List<List<String>>, List<SelectedColumnMeta>> createResponseFromResultSet(ResultSet resultSet) throws SQLException{
        List<List<String>> results = Lists.newArrayList();
        List<SelectedColumnMeta> columnMetas = Lists.newArrayList();

        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        // Fill in selected column meta
        for (int i = 1; i <= columnCount; ++i) {
            columnMetas.add(new SelectedColumnMeta(metaData.isAutoIncrement(i), metaData.isCaseSensitive(i),
                    metaData.isSearchable(i), metaData.isCurrency(i), metaData.isNullable(i), metaData.isSigned(i),
                    metaData.getColumnDisplaySize(i), metaData.getColumnLabel(i), metaData.getColumnName(i),
                    metaData.getSchemaName(i), metaData.getCatalogName(i), metaData.getTableName(i),
                    metaData.getPrecision(i), metaData.getScale(i), metaData.getColumnType(i),
                    metaData.getColumnTypeName(i), metaData.isReadOnly(i), metaData.isWritable(i),
                    metaData.isDefinitelyWritable(i)));
        }

        // fill in results
        while (resultSet.next()) {
            List<String> oneRow = Lists.newArrayListWithCapacity(columnCount);
            for (int i = 0; i < columnCount; i++) {
                oneRow.add((resultSet.getString(i + 1)));
            }

            results.add(oneRow);
        }

        return new Pair<>(results, columnMetas);
    }

    private static SQLResponse buildSqlResponse(String projectName, Boolean isPushDown, List<List<String>> results,
                                         List<SelectedColumnMeta> columnMetas) {
        return buildSqlResponse(projectName, isPushDown, results, columnMetas, false, null);
    }

    private static SQLResponse buildSqlResponse(String projectName, Boolean isPushDown, List<List<String>> results,
                                         List<SelectedColumnMeta> columnMetas, boolean isException, String exceptionMessage) {

        boolean isPartialResult = false;

        List<String> realizations = Lists.newLinkedList();
        StringBuilder cubeSb = new StringBuilder();
        StringBuilder logSb = new StringBuilder("Processed rows for each storageContext: ");
        QueryContext queryContext = QueryContextFacade.current();
        if (OLAPContext.getThreadLocalContexts() != null) { // contexts can be null in case of 'explain plan for'
            for (OLAPContext ctx : OLAPContext.getThreadLocalContexts()) {
                String realizationName = "NULL";
                int realizationType = -1;
                if (ctx.realization != null) {
                    isPartialResult |= ctx.storageContext.isPartialResultReturned();
                    if (cubeSb.length() > 0) {
                        cubeSb.append(",");
                    }
                    cubeSb.append(ctx.realization.getCanonicalName());
                    logSb.append(ctx.storageContext.getProcessedRowCount()).append(" ");

                    realizationName = ctx.realization.getName();
                    realizationType = ctx.realization.getStorageType();

                    realizations.add(realizationName);
                }
                queryContext.setContextRealization(ctx.id, realizationName, realizationType);
            }
        }
        logger.info(logSb.toString());

        SQLResponse response = new SQLResponse(columnMetas, results, cubeSb.toString(), 0, isException,
                exceptionMessage, isPartialResult, isPushDown);
        response.setTotalScanCount(queryContext.getScannedRows());
        response.setTotalScanFiles((queryContext.getScanFiles() < 0) ? -1 :
                queryContext.getScanFiles());
        response.setMetadataTime((queryContext.getMedataTime() < 0) ? -1 :
                queryContext.getMedataTime());
        response.setTotalSparkScanTime((queryContext.getScanTime() < 0) ? -1 :
                queryContext.getScanTime());
        response.setTotalScanBytes((queryContext.getScannedBytes() < 0) ?
                (queryContext.getSourceScanBytes() < 1 ? -1 : queryContext.getSourceScanBytes()) : queryContext.getScannedBytes());
        response.setCubeSegmentStatisticsList(queryContext.getCubeSegmentStatisticsResultList());
        response.setSparkPool(queryContext.getSparkPool());
        return response;
    }

    private static String getCompareSql(String originSqlPath) {
        if (!originSqlPath.endsWith(".sql")) {
            return "";
        }
        File file = new File(originSqlPath + ".expected");
        if (!file.exists())
            return "";

        try {
            return FileUtils.readFileToString(file, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("meet error when reading compared spark sql from {}", file.getAbsolutePath());
            return "";
        }
    }

    public static List<List<String>> queryCubeWithJDBC(String prj, String sql) throws Exception {
        //      KylinSparkEnv.skipCompute();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        List<List<String>> results = Lists.newArrayList();
        try {
            conn = QueryConnection.getConnection(prj);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<String> oneRow = Lists.newArrayListWithCapacity(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    oneRow.add((rs.getString(i + 1)));
                }

                results.add(oneRow);
            }
        } finally {
            DBUtils.closeQuietly(rs);
            DBUtils.closeQuietly(stmt);
            DBUtils.closeQuietly(conn);
        }
        return results;
    }

    public static String changeJoinType(String sql, String targetType) {

        if (targetType.equalsIgnoreCase("default")) {
            return sql;
        }

        String specialStr = "changeJoinType_DELIMITERS";
        sql = sql.replaceAll(System.getProperty("line.separator"), " " + specialStr + " ");

        String[] tokens = StringUtils.split(sql, null);// split white spaces
        for (int i = 0; i < tokens.length - 1; ++i) {
            if ((tokens[i].equalsIgnoreCase("inner") || tokens[i].equalsIgnoreCase("left"))
                    && tokens[i + 1].equalsIgnoreCase("join")) {
                tokens[i] = targetType.toLowerCase(Locale.ROOT);
            }
        }

        String ret = StringUtils.join(tokens, " ");
        ret = ret.replaceAll(specialStr, System.getProperty("line.separator"));
        logger.trace("The actual sql executed is: " + ret);

        return ret;
    }

    static class ITQueryMetrics {
        private long scanRowCount;
        private long scanBytes;
        private long scanFiles;
        private List<Long> cuboidId;
        private List<Boolean> exactlyMatched;

        public ITQueryMetrics() {
            this.scanRowCount = -1L;
            this.scanBytes = -1L;
            this.scanFiles = -1L;
            this.cuboidId = new ArrayList<>();
            this.exactlyMatched = new ArrayList<>();
        }

        public ITQueryMetrics(long scanRowCount, long scanBytes, long scanFiles,
                              List<Long> cuboidId, List<Boolean> exactlyMatched) {
            this.scanRowCount = scanRowCount;
            this.scanBytes = scanBytes;
            this.scanFiles = scanFiles;
            this.cuboidId = cuboidId;
            this.exactlyMatched = exactlyMatched;
        }

        public long getScanRowCount() {
            return scanRowCount;
        }

        public void setScanRowCount(long scanRowCount) {
            this.scanRowCount = scanRowCount;
        }

        public long getScanBytes() {
            return scanBytes;
        }

        public void setScanBytes(long scanBytes) {
            this.scanBytes = scanBytes;
        }

        public long getScanFiles() {
            return scanFiles;
        }

        public void setScanFiles(long scanFiles) {
            this.scanFiles = scanFiles;
        }

        public List<Long> getCuboidId() {
            return cuboidId;
        }

        public void setCuboidId(List<Long> cuboidId) {
            this.cuboidId = cuboidId;
        }

        public List<Boolean> getExactlyMatched() {
            return exactlyMatched;
        }

        public void setExactlyMatched(List<Boolean> exactlyMatched) {
            this.exactlyMatched = exactlyMatched;
        }

        public boolean equals(ITQueryMetrics metrics) {
            return this.cuboidId.equals(metrics.getCuboidId())
                    && this.scanFiles == metrics.getScanFiles()
                    && this.scanRowCount == metrics.getScanRowCount();
        }

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer("QueryMetrics: ");
            sb.append("cuboidId=").append(this.getCuboidId()).append(",");
            sb.append("exactlyMatched=").append(this.getExactlyMatched()).append(",");
            sb.append("scanBytes=").append(this.getScanBytes()).append(",");
            sb.append("scanFiles=").append(this.getScanFiles()).append(",");
            sb.append("scanRowCount=").append(this.getScanRowCount());
            return sb.toString();
        }
    }
}

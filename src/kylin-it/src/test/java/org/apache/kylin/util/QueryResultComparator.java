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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.query.StructField;
import org.apache.kylin.query.engine.data.QueryResult;
import org.apache.spark.sql.common.SparderQueryTest;
import org.junit.Assert;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryResultComparator {

    public static void compareColumnType(List<StructField> modelSchema, List<StructField> sparkSchema) {
        val cubeSize = modelSchema.size();
        val sparkSize = sparkSchema.size();
        Assert.assertEquals(cubeSize + " did not equal " + sparkSize, sparkSize, cubeSize);
        for (int i = 0; i < cubeSize; i++) {
            val cubeStructField = modelSchema.get(i);
            val sparkStructField = sparkSchema.get(i);
            Assert.assertTrue(
                    cubeStructField.getDataTypeName() + " did not equal " + sparkSchema.get(i).getDataTypeName(),
                    SparderQueryTest.isSameDataType(cubeStructField, sparkStructField));
        }
    }

    public static boolean compareResults(QueryResult expectedResult, QueryResult actualResult,
            ExecAndComp.CompareLevel compareLevel) {
        boolean good = true;

        val expectedRows = normalizeResult(expectedResult.getRows(), actualResult.getColumns());
        val actualRows = normalizeResult(actualResult.getRows(), actualResult.getColumns());

        switch (compareLevel) {
        case SAME_ORDER:
            good = expectedRows.equals(actualRows);
            break;
        case SAME:
            good = compareResultInLevelSame(expectedRows, actualRows);
            break;
        case SAME_ROWCOUNT:
            good = expectedRows.size() == actualRows.size();
            break;
        case SUBSET: {
            good = compareResultInLevelSubset(expectedRows, actualRows);
            break;
        }
        default:
            break;
        }

        if (!good) {
            log.error("Result not match");
            printRows("expected", expectedRows);
            printRows("actual", actualRows);
        }
        return good;
    }

    private static List<String> normalizeResult(List<List<String>> rows, List<StructField> columns) {
        List<String> resultRows = Lists.newArrayList();
        for (List<String> row : rows) {
            StringBuilder normalizedRow = new StringBuilder();
            for (int i = 0; i < columns.size(); i++) {
                StructField column = columns.get(i);
                if (row.get(i) == null || row.get(i).equals("null")) {
                    normalizedRow.append("");
                } else if (row.get(i).equals("NaN") || row.get(i).contains("Infinity")) {
                    normalizedRow.append(row.get(i));
                } else if (column.getDataTypeName().equals("DOUBLE")
                        || column.getDataTypeName().startsWith("DECIMAL")) {
                    normalizedRow.append(new BigDecimal(row.get(i)).setScale(2, RoundingMode.HALF_UP));
                } else if (column.getDataTypeName().equals("DATE")) {
                    val mills = DateFormat.stringToMillis(row.get(i));
                    normalizedRow.append(DateFormat.formatToDateStr(mills));
                } else if (column.getDataTypeName().equals("TIMESTAMP")) {
                    val millis = DateFormat.stringToMillis(row.get(i));
                    normalizedRow.append(DateFormat.castTimestampToString(millis));
                } else if (column.getDataTypeName().equals("ANY")) {
                    // bround udf return ANY
                    try {
                        normalizedRow.append(new BigDecimal(row.get(i)).setScale(2, RoundingMode.HALF_UP));
                    } catch (Exception e) {
                        log.warn("try to cast to decimal failed", e);
                        normalizedRow.append(row.get(i));
                    }
                } else {
                    normalizedRow.append(row.get(i));
                }
                normalizedRow.append(" | ");
            }
            resultRows.add(normalizedRow.toString());
        }
        return resultRows;
    }

    private static boolean compareResultInLevelSubset(List<String> expectedResult, List<String> actualResult) {
        return CollectionUtils.isSubCollection(actualResult, expectedResult);
    }

    private static boolean compareResultInLevelSame(List<String> expectedResult, List<String> actualResult) {
        return CollectionUtils.isEqualCollection(expectedResult, actualResult);
    }

    private static void printRows(String source, List<String> rows) {
        log.info("***********" + source + " start, only show top 100 result**********");
        rows.stream().limit(100).forEach(log::info);
        log.info("***********" + source + " end**********");
    }
}

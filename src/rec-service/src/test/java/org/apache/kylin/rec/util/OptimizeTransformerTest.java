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

package org.apache.kylin.rec.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@MetadataInfo
class OptimizeTransformerTest extends AbstractTestCase {
    private static final String PROJECT = "default";

    @BeforeEach
    void setup() {
        overwriteSystemPropBeforeClass("kylin.smart.conf.optimize-transformer-condition-count-threshold", "0");
    }

    @Test
    void testSimpleSql() {
        OptimizeTransformer transformer = new OptimizeTransformer();

        // base test
        String originSql = "SELECT column1, column2 FROM t1 WHERE\n\t column1 IN (1, -2, 'two' , A) AND column2 IN ('four', 5, 'six', C)";
        String expectSql = "SELECT column1, column2 FROM t1 WHERE column1 IN (1, A) AND column2 IN ('four', C)";
        String transformed = transformer.transform(originSql, PROJECT, null);
        Assertions.assertEquals(expectSql, transformed);

        // test parenthesized query
        originSql = "SELECT (column1, 1, 2, 'a') FROM t1 WHERE column1 IN (1, 'two', A) AND column2 IN ('four', 5, 'six', C)";
        expectSql = "SELECT (column1, 1, 2, 'a') FROM t1 WHERE column1 IN (1, A) AND column2 IN ('four', C)";
        transformed = transformer.transform(originSql, PROJECT, null);
        Assertions.assertEquals(expectSql, transformed);

        // test unescaped sql
        originSql = "select {FN CONVERT(PRICE, SQL_BIGINT)}, N'foo', x'A09', X'B11', _UTF16'foo', U&'foo' from tbl where bar = N'a' and bar in (N'foo', x'A09', X'B11', _UTF16'foo', U&'foo')";
        expectSql = "select {FN CONVERT(PRICE, SQL_BIGINT)}, N'foo', x'A09', X'B11', _UTF16'foo', U&'foo' from tbl where bar = N'a' and bar = N'foo'";
        transformed = transformer.transform(originSql, PROJECT, null);
        Assertions.assertEquals(expectSql, transformed);

        // test data time
        originSql = "SELECT a FROM t1 WHERE column1 IN (1, 'two', A, time'08:00:00', DATE '2001-01-01', timestamp '2011-02-13 07:57:12')";
        expectSql = "SELECT a FROM t1 WHERE column1 IN (1, A)";
        transformed = transformer.transform(originSql, PROJECT, null);
        Assertions.assertEquals(expectSql, transformed);

        // test data types
        originSql = "SELECT a FROM t1 WHERE column1 IN (1, 'two', 2, .6788, 1.2e8, A, time'08:00:00', true, false, null)";
        expectSql = "SELECT a FROM t1 WHERE column1 IN (1, A)";
        transformed = transformer.transform(originSql, PROJECT, null);
        Assertions.assertEquals(expectSql, transformed);

        // test in subquery
        originSql = "SELECT a FROM t1 WHERE A IN (select id from t2 where c in (1,2,3) or d in (1,2) or c in (3,4,5)) and b in (1,2,3)";
        expectSql = "SELECT a FROM t1 WHERE A IN (select id from t2 where d = 1 or c = 3 ) and b = 1";
        transformed = transformer.transform(originSql, PROJECT, null);
        Assertions.assertEquals(expectSql, transformed);

        // test CubePriority and Hint
        originSql = "-- CubePriority(model1,model2)\n SELECT /*+ MODEL_PRIORITY(model1, model2) */ a FROM t1 WHERE b in (1,2,3)";
        expectSql = "-- CubePriority(model1,model2)\n SELECT /*+ MODEL_PRIORITY(model1, model2) */ a FROM t1 WHERE b = 1";
        transformed = transformer.transform(originSql, PROJECT, null);
        Assertions.assertEquals(expectSql, transformed);
    }

    @Test
    void testInCommaListNesting() {
        String originSql = "SELECT a FROM t1 WHERE column1 IN (1, 'two', A, (1,2,3), ('a','b','c')) AND "
                + "column2 IN ('four', 5, 'six', case when a in (1,2,3,a,c,'a','b') then 1 else 0 end, 1, 2, 3) ";
        String expectSql = "SELECT a FROM t1 WHERE column1 IN (1, A, (1,2,3), ('a','b','c')) AND "
                + "column2 IN ('four', case when a in (1,2,3,a,c,'a','b') then 1 else 0 end)";
        OptimizeTransformer transformer = new OptimizeTransformer();
        String transformed = transformer.transform(originSql, PROJECT, null);
        Assertions.assertEquals(expectSql, transformed);
    }

    @Test
    void testMergeOrCondition() {
        String originSql = "SELECT a FROM t1 WHERE a in (1, x) or a in (2, y) or a in (3, z) or a in (4) or a in (5) and a in (6)";
        String expectSql = "SELECT a FROM t1 WHERE a IN (1, x, y, z) or a = 5 and a = 6";
        OptimizeTransformer transformer = new OptimizeTransformer();
        String transformed = transformer.transform(originSql, PROJECT, null);
        Assertions.assertEquals(expectSql, transformed);

        originSql = "SELECT a FROM t1 WHERE a in (1) or a = 3 or a in (2)";
        expectSql = "SELECT a FROM t1 WHERE a = 1 or a = 2";
        transformed = transformer.transform(originSql, PROJECT, null);
        Assertions.assertEquals(expectSql, transformed);

        originSql = "SELECT a FROM t1 WHERE a = 1 or a = 3 or a = 2";
        expectSql = "SELECT a FROM t1 WHERE a = 1 or a = 2";
        transformed = transformer.transform(originSql, PROJECT, null);
        Assertions.assertEquals(expectSql, transformed);

        originSql = "SELECT a FROM t1 WHERE a = 1 or a in (3) or a = 2";
        expectSql = "SELECT a FROM t1 WHERE a = 3 or a = 2";
        transformed = transformer.transform(originSql, PROJECT, null);
        Assertions.assertEquals(expectSql, transformed);

        originSql = "SELECT a FROM t1 WHERE 1 = a or a in (3) or 2 = a";
        expectSql = "SELECT a FROM t1 WHERE a = 3 or 2 = a";
        transformed = transformer.transform(originSql, PROJECT, null);
        Assertions.assertEquals(expectSql, transformed);

        originSql = "select * from t1 where (a = 1 or (a = 3)) or (a = 4 or a = 7) or a = 5";
        expectSql = "select * from t1 where ( a = 1 ) or a = 5";
        transformed = transformer.transform(originSql, PROJECT, null);
        Assertions.assertEquals(expectSql, transformed);
    }

    @Test
    void testBigSql() throws IOException {
        File sqlFile = new File("./src/test/resources/ut_big_sqls/sqls1.sql");
        String originSql = FileUtils.readFileToString(sqlFile, StandardCharsets.UTF_8);
        File expectFile = new File("./src/test/resources/ut_big_sqls/sqls1.expect");
        String expectSql = FileUtils.readFileToString(expectFile, StandardCharsets.UTF_8);
        OptimizeTransformer transformer = new OptimizeTransformer();
        String transformed = transformer.transform(originSql, PROJECT, null);
        Assertions.assertEquals(expectSql, transformed);
    }
}

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

package org.apache.kylin.query.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.io.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CognosParenthesesEscapeTransformerTest extends AbstractTestCase {

    @Before
    public void setUp() throws Exception {
        overwriteSystemProp("spark.local", "true");
        File tmpHome = Files.createTempDir();
        overwriteSystemProp("KYLIN_HOME", tmpHome.getAbsolutePath());
        FileUtils.touch(new File(tmpHome.getAbsolutePath() + "/kylin.properties"));
        KylinConfig.setKylinConfigForLocalTest(tmpHome.getCanonicalPath());
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.setProperty("kylin.env", "UT");
    }

    @Test
    public void basicTest() {
        CognosParenthesesEscapeTransformer escape = new CognosParenthesesEscapeTransformer();
        String data = " from ((a left outer join b on a.x1 = b.y1 and a.x2=b.y2 and   a.x3= b.y3) inner join c as cc on a.x1=cc.z1 ) join d dd on a.x1=d.w1 and a.x2 =d.w2 ";
        String expected = " from a left outer join b on a.x1 = b.y1 and a.x2=b.y2 and   a.x3= b.y3 inner join c as cc on a.x1=cc.z1  join d dd on a.x1=d.w1 and a.x2 =d.w2 ";
        String transformed = escape.completion(data);
        Assert.assertEquals(expected, transformed);
    }

    @Test
    public void advanced1Test() throws IOException {
        advancedTestTemplate("src/test/resources/query/cognos/query01.sql",
                "src/test/resources/query/cognos/query01.sql.expected");
    }

    @Test
    public void advanced2Test() throws IOException {
        advancedTestTemplate("src/test/resources/query/cognos/query02.sql",
                "src/test/resources/query/cognos/query02.sql.expected");
    }

    @Test
    public void advanced3Test() throws IOException {
        advancedTestTemplate("src/test/resources/query/cognos/query03.sql",
                "src/test/resources/query/cognos/query03.sql.expected");
    }

    private void advancedTestTemplate(String originFile, String expectedFile) throws IOException {
        CognosParenthesesEscapeTransformer escape = new CognosParenthesesEscapeTransformer();
        String query = FileUtils.readFileToString(new File(originFile), Charset.defaultCharset());
        String expected = FileUtils.readFileToString(new File(expectedFile), Charset.defaultCharset());
        String transformed = escape.completion(query);
        Assert.assertEquals(expected, transformed);
    }

    @Test
    public void proguardTest() throws IOException {
        CognosParenthesesEscapeTransformer escape = new CognosParenthesesEscapeTransformer();
        Collection<File> files = FileUtils.listFiles(new File("../kylin-it/src/test/resources/query"),
                new String[] { "sql" }, true);
        for (File f : files) {
            System.out.println("checking " + f.getCanonicalPath());
            if (f.getCanonicalPath().contains("sql_parentheses_escape")) {
                continue;
            }
            if (f.getCanonicalPath().contains("sql_count_distinct_expr")) {
                continue;
            }
            // KAP#16063 CognosParenthesesEscapeTransformer wrongly removes
            // parentheses for sql in sql_except directory
            // exclude sql_except directory from the test until the issue is fixed
            if (f.getCanonicalPath().contains("sql_except")) {
                continue;
            }
            String query = FileUtils.readFileToString(f, Charset.defaultCharset());
            String transformed = escape.completion(query);
            Assert.assertEquals(query, transformed);
        }
    }

    @Test
    public void advanced4Test() throws Exception {
        CognosParenthesesEscapeTransformer converter = new CognosParenthesesEscapeTransformer();

        String originalSql = "select count(*) COU FrOm --comment0\n" + "---comment1\n" + "(  --comment2\n"
                + "test_kylin_fact left join EDW.TEST_CAL_DT on TEST_CAL_DT.CAL_DT = test_kylin_fact.CAL_DT\n" + ")\n"
                + "left join TEST_ACCOUNT on SELLER_ID = ACCOUNT_ID";
        String expectedSql = "select count(*) COU FrOm\n" + "\n"
                + "test_kylin_fact left join EDW.TEST_CAL_DT on TEST_CAL_DT.CAL_DT = test_kylin_fact.CAL_DT\n" + "\n"
                + "left join TEST_ACCOUNT on SELLER_ID = ACCOUNT_ID";
        originalSql = QueryUtil.removeCommentInSql(originalSql);
        String transformed = converter.completion(originalSql);
        Assert.assertEquals(expectedSql, transformed);

        String transformedSecond = converter.completion(transformed);
        Assert.assertEquals(expectedSql, transformedSecond);
    }

    @Test
    public void advanced5Test() throws Exception {
        CognosParenthesesEscapeTransformer convertTransformer = new CognosParenthesesEscapeTransformer();
        String sql2 = FileUtils.readFileToString(
                new File("../kylin-it/src/test/resources/query/sql_parentheses_escape/query05.sql"),
                Charset.defaultCharset());
        String expectedSql2 = FileUtils.readFileToString(
                new File("../kylin-it/src/test/resources/query/sql_parentheses_escape/query05.sql.expected"),
                Charset.defaultCharset());
        sql2 = QueryUtil.removeCommentInSql(sql2);
        String transform2 = convertTransformer.completion(sql2).replaceAll("[\n]+", "");
        expectedSql2 = expectedSql2.replaceAll("[\n]+", "");
        Assert.assertEquals(expectedSql2, transform2);
    }

    @Test
    public void advanced6Test() throws Exception {
        CognosParenthesesEscapeTransformer convertTransformer = new CognosParenthesesEscapeTransformer();

        String originalSql = FileUtils.readFileToString(
                new File("../kylin-it/src/test/resources/query/sql_parentheses_escape/query06.sql"),
                Charset.defaultCharset());
        String expectedSql = FileUtils.readFileToString(
                new File("../kylin-it/src/test/resources/query/sql_parentheses_escape/query06.sql.expected"),
                Charset.defaultCharset());
        originalSql = QueryUtil.removeCommentInSql(originalSql);
        String transformed = convertTransformer.completion(originalSql).replaceAll("[\n]+", "");
        expectedSql = expectedSql.replaceAll("[\n]+", "").substring(0, expectedSql.length() - 1);
        Assert.assertEquals(expectedSql, transformed);
    }
}

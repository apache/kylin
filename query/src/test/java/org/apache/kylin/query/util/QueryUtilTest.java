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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueryUtilTest extends LocalFileMetadataTestCase {

    static final String catalog = "CATALOG";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testappendLimitOffsetToSql() {
        {
            String sql = "select ( date '2001-09-28' + interval floor(1.2) day) from test_kylin_fact";
            String newsql = QueryUtil.appendLimitOffsetToSql(sql, 100, 100);
            Assert.assertEquals(
                    "select * from (select ( date '2001-09-28' + interval floor(1.2) day) from test_kylin_fact) limit 100 offset 100",
                    newsql);
        }

        {
            String sql = "select ( date '2001-09-28' + interval floor(1.2) day) from test_kylin_fact";
            String newsql = QueryUtil.appendLimitOffsetToSql(sql, 0, 0);
            Assert.assertEquals(
                    "select ( date '2001-09-28' + interval floor(1.2) day) from test_kylin_fact",
                    newsql);
        }

        {
            String sql = "explain plan for select ( date '2001-09-28' + interval floor(1.2) day) from test_kylin_fact";
            String newsql = QueryUtil.appendLimitOffsetToSql(sql, 100, 100);
            Assert.assertEquals(
                    "explain plan for select ( date '2001-09-28' + interval floor(1.2) day) from test_kylin_fact limit 100 offset 100",
                    newsql);
        }

        {
            String sql = "explain plan for select ( date '2001-09-28' + interval floor(1.2) day) from test_kylin_fact";
            String newsql = QueryUtil.appendLimitOffsetToSql(sql, 0, 0);
            Assert.assertEquals(
                    "explain plan for select ( date '2001-09-28' + interval floor(1.2) day) from test_kylin_fact",
                    newsql);
        }
    }

    @Test
    public void testMassageSql() {
        {
            String sql = "select ( date '2001-09-28' + interval floor(1.2) day) from test_kylin_fact";
            String s = QueryUtil.massageSql(sql, "default", 0, 0, "DEFAULT");
            Assert.assertEquals("select ( date '2001-09-28' + interval '1' day) from test_kylin_fact", s);
        }
        {
            String sql = "select ( date '2001-09-28' + interval floor(2) month) from test_kylin_fact group by ( date '2001-09-28' + interval floor(2) month)";
            String s = QueryUtil.massageSql(sql, "default", 0, 0, "DEFAULT");
            Assert.assertEquals(
                    "select ( date '2001-09-28' + interval '2' month) from test_kylin_fact group by ( date '2001-09-28' + interval '2' month)",
                    s);
        }
        {
            String sql = "select count(*) test_limit from test_kylin_fact where price > 10.0";
            String s = QueryUtil.massageSql(sql, "default", 50000, 0, "DEFAULT");
            Assert.assertEquals(
                    "select * from (select count(*) test_limit from test_kylin_fact where price > 10.0) limit 50000",
                    s);
        }
        {
            String sql = "select count(*) test_offset from test_kylin_fact where price > 10.0";
            String s = QueryUtil.massageSql(sql, "default", 0, 50, "DEFAULT");
            Assert.assertEquals(
                    "select * from (select count(*) test_offset from test_kylin_fact where price > 10.0) offset 50",
                    s);
        }
        {
            String sql = "select count(*) test_limit_and_offset from test_kylin_fact where price > 10.0";
            String s = QueryUtil.massageSql(sql, "default", 50000, 50, "DEFAULT");
            Assert.assertEquals(
                    "select * from (select count(*) test_limit_and_offset from test_kylin_fact where price > 10.0) limit 50000 offset 50",
                    s);
        }

        {
            String newLine = System.getProperty("line.separator");
            String sql = "select count(*)     test_limit from " + newLine + "test_kylin_fact where price > 10.0";
            newLine = newLine.replace("\r", " ").replace("\n", newLine);
            String s = QueryUtil.massageSql(sql, "default", 50000, 0, "DEFAULT");
            Assert.assertEquals(
                    "select * from (select count(*)     test_limit from " + newLine + "test_kylin_fact where price > 10.0) limit 50000",
                    s);
        }
        {
            String newLine = System.getProperty("line.separator");
            String sql = "select count(*)     test_offset from " + newLine + "test_kylin_fact where price > 10.0";
            newLine = newLine.replace("\r", " ").replace("\n", newLine);
            String s = QueryUtil.massageSql(sql, "default", 50000, 0, "DEFAULT");
            Assert.assertEquals(
                    "select * from (select count(*)     test_offset from " + newLine + "test_kylin_fact where price > 10.0) limit 50000",
                    s);
        }
        {
            String newLine = System.getProperty("line.separator");
            String sql = "select count(*)     test_limit_and_offset from " + newLine + "test_kylin_fact where price > 10.0";
            newLine = newLine.replace("\r", " ").replace("\n", newLine);
            String s = QueryUtil.massageSql(sql, "default", 50000, 0, "DEFAULT");
            Assert.assertEquals(
                    "select * from (select count(*)     test_limit_and_offset from " + newLine + "test_kylin_fact where price > 10.0) limit 50000",
                    s);
        }
    }

    @Test
    public void testIsSelect() {
        {
            String sql = "select ( date '2001-09-28' + interval floor(1.2) day) from test_kylin_fact";
            boolean selectStatement = QueryUtil.isSelectStatement(sql);
            Assert.assertEquals(true, selectStatement);
        }
        {
            String sql = " Select ( date '2001-09-28' + interval floor(1.2) day) from test_kylin_fact";
            boolean selectStatement = QueryUtil.isSelectStatement(sql);
            Assert.assertEquals(true, selectStatement);
        }
        {
            String sql = " \n" + "Select ( date '2001-09-28' + interval floor(1.2) day) from test_kylin_fact";
            boolean selectStatement = QueryUtil.isSelectStatement(sql);
            Assert.assertEquals(true, selectStatement);
        }
        {
            String sql = "--comment\n"
                    + " /* comment */Select ( date '2001-09-28' + interval floor(1.2) day) from test_kylin_fact";
            boolean selectStatement = QueryUtil.isSelectStatement(sql);
            Assert.assertEquals(true, selectStatement);
        }
        {
            String sql = " UPDATE Customers\n" + "SET ContactName = 'Alfred Schmidt', City= 'Frankfurt'\n"
                    + "WHERE CustomerID = 1;";
            boolean selectStatement = QueryUtil.isSelectStatement(sql);
            Assert.assertEquals(false, selectStatement);
        }
        {
            String sql = " explain plan for select count(*) from test_kylin_fact\n";
            boolean selectStatement = QueryUtil.isSelectStatement(sql);
            Assert.assertEquals(true, selectStatement);
        }
    }

    @Test
    public void testKeywordDefaultDirtyHack() {
        {
            KylinConfig.getInstanceFromEnv().setProperty("kylin.query.escape-default-keyword", "true");
            String sql = "select * from DEFAULT.TEST_KYLIN_FACT";
            String s = QueryUtil.massageSql(sql, "default", 0, 0, "DEFAULT");
            Assert.assertEquals("select * from \"DEFAULT\".TEST_KYLIN_FACT", s);
        }
    }

    @Test
    public void testForceLimit() {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.force-limit", "10");
        String sql1 = "select   * \nfrom DEFAULT.TEST_KYLIN_FACT";
        String result = QueryUtil.massageSql(sql1, "default", 0, 0, "DEFAULT");
        Assert.assertEquals("select * from (select   * \nfrom DEFAULT.TEST_KYLIN_FACT) limit 10", result);

        String sql2 = "select   2 * 8 from DEFAULT.TEST_KYLIN_FACT";
        result = QueryUtil.massageSql(sql2, "default", 0, 0, "DEFAULT");
        Assert.assertEquals("select   2 * 8 from DEFAULT.TEST_KYLIN_FACT", result);
    }

    @Test
    public void testRemoveCommentInSql() {

        final String originSql = "select count(*) from test_kylin_fact where price > 10.0";
        final String originSql2 = "select count(*) from test_kylin_fact where TEST_COLUMN != 'not--a comment'";

        {
            String sqlWithComment = "-- comment \n" + originSql;
            Assert.assertEquals(originSql, QueryUtil.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "-- comment \n -- comment\n" + originSql;
            Assert.assertEquals(originSql, QueryUtil.removeCommentInSql(sqlWithComment));
        }

        {

            String sqlWithComment = "-- \n -- comment \n" + originSql;
            Assert.assertEquals(originSql, QueryUtil.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = originSql + "-- \n -- comment \n";
            Assert.assertEquals(originSql, QueryUtil.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "-- \n -- comment \n" + originSql + "-- \n -- comment \n";
            Assert.assertEquals(originSql, QueryUtil.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "/* comment */ " + originSql + "-- \n -- comment \n";
            Assert.assertEquals(originSql, QueryUtil.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "/* comment1/comment2 */ " + originSql;
            Assert.assertEquals(originSql, QueryUtil.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "/* comment1 * comment2 */ " + originSql;
            Assert.assertEquals(originSql, QueryUtil.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "/* comment1 * comment2 */ /* comment3 / comment4 */ -- comment 5\n" + originSql;
            Assert.assertEquals(originSql, QueryUtil.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "/* comment1 * \ncomment2 */ -- comment 5\n" + originSql + "/* comment3 / comment4 */";
            Assert.assertEquals(originSql, QueryUtil.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "/* comment1 * \ncomment2 */ -- comment 3\n" + originSql + "-- comment 5\n";
            Assert.assertEquals(originSql, QueryUtil.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment2 = "/* comment1 * \ncomment2 */ -- comment 5\n" + originSql2 + "/* comment3 / comment4 */";
            Assert.assertEquals(originSql2, QueryUtil.removeCommentInSql(sqlWithComment2));
        }

        {
            String sqlWithComment2 = "/* comment1 * comment2 */ /* comment3 / comment4 */ -- comment 5\n" + originSql2;
            Assert.assertEquals(originSql2, QueryUtil.removeCommentInSql(sqlWithComment2));
        }

        {
            String sqlWithComment2 = "/* comment1 * comment2 */ " + originSql2;
            Assert.assertEquals(originSql2, QueryUtil.removeCommentInSql(sqlWithComment2));
        }

        {
            String sqlWithComment2 = "/* comment1/comment2 */ " + originSql2;
            Assert.assertEquals(originSql2, QueryUtil.removeCommentInSql(sqlWithComment2));
        }

        {
            String sqlWithComment2 = "-- \n -- comment \n" + originSql2;
            Assert.assertEquals(originSql2, QueryUtil.removeCommentInSql(sqlWithComment2));
        }

        {
            String sqlWithComment2 = "-- comment \n" + originSql2;
            Assert.assertEquals(originSql2, QueryUtil.removeCommentInSql(sqlWithComment2));
        }

        {
            String sqlWithComment2 = "-- comment \n -- comment\n" + originSql2;
            Assert.assertEquals(originSql2, QueryUtil.removeCommentInSql(sqlWithComment2));
        }

        {
            String content = "        --  One-line comment and /**range\n" +
                    "/*\n" +
                    "Multi-line comment\r\n" +
                    "--  Multi-line comment*/\n" +
                    "select price as " +
                    "/*\n" +
                    "Multi-line comment\r\n" +
                    "--  Multi-line comment*/\n" +
                    "revenue from /*One-line comment-- One-line comment*/ v_lineitem;";
            String expectedContent = "select price as revenue from  v_lineitem;";
            String trimmedContent = QueryUtil.removeCommentInSql(content).replaceAll("\n", "").trim();
            Assert.assertEquals(trimmedContent, expectedContent);
        }

        {
            String sqlWithComment = "select count(*) from test_kylin_fact WHERE column_name = '--this is not comment'\n " +
                    "LIMIT 100 offset 0";
            Assert.assertEquals(sqlWithComment, QueryUtil.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "select count(*) from test_kylin_fact WHERE column_name = '--this is not comment'";
            Assert.assertEquals(sqlWithComment, QueryUtil.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "select count(*) from test_kylin_fact WHERE column_name = '/*--this is not comment*/'";
            Assert.assertEquals(sqlWithComment, QueryUtil.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "-- comment \n" + originSql + "/* comment */;" + "-- comment \n" + originSql + "/* comment */";
            Assert.assertEquals("select count(*) from test_kylin_fact where price > 10.0;\n" +
                    "select count(*) from test_kylin_fact where price > 10.0", QueryUtil.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "select count(*) from test_kylin_fact /* this is test*/  where price > 10.0 -- comment \n" +
                    ";" + "insert into test_kylin_fact(id) values(?); -- comment \n";
            Assert.assertEquals("select count(*) from test_kylin_fact   where price > 10.0 \n" +
                    ";insert into test_kylin_fact(id) values(?);", QueryUtil.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithoutComment = "select * from test_kylin_fact where price=\"/* this is not comment */\"";
            Assert.assertEquals(sqlWithoutComment, QueryUtil.removeCommentInSql(sqlWithoutComment));
        }

        {
            String sqlWithoutComment = "select count(*) from test_kylin_fact -- 'comment'\n";
            Assert.assertEquals("select count(*) from test_kylin_fact", QueryUtil.removeCommentInSql(sqlWithoutComment));
        }
        {
            Assert.assertEquals("select count(*)",
                    QueryUtil.removeCommentInSql("select count(*) -- , --\t --/ --"));
        }
        {
            Assert.assertEquals("select 1\t''", QueryUtil.removeCommentInSql("select 1\t''/* comment */"));
        }
        {
            Assert.assertEquals("select ' \t\n,\r/-'", QueryUtil.removeCommentInSql("select ' \t\n,\r/-'"));
        }
        {
            Assert.assertEquals("select ' \t\n,\r/-'", QueryUtil.removeCommentInSql("select ' \t\n,\r/-'--comment"));
        }
        {
            Assert.assertEquals("select ' \t\n,\r/-'", QueryUtil.removeCommentInSql("select ' \t\n,\r/-'/*comment*/"));
        }
    }

    @Test
    public void testUnknownErrorResponseMessage() {
        String msg = QueryUtil.makeErrorMsgUserFriendly(new NullPointerException());
        Assert.assertEquals("Unknown error.", msg);
    }

    @Test
    public void testRemoveCatalog() {

        String[] beforeRemoveSql = new String[] {
                "select name, count(*) as cnt from schema1.user where bb.dd >2 group by name",
                "select name, count(*) as cnt from .default2.user where dd >2 group by name",
                "select name, count(*) as cnt from %s.default2.user where dd >2 group by name",
                "select name, count(*) as cnt from %s.user.a.cu where dd >2 group by name",
                "select name, count(*) as cnt from %s.default2.user where dd >2 group by name",
                "select name, count() as cnt from %s.test.kylin_sales inner join " + "%s.test.kylin_account "
                        + "ON kylin_sales.BUYER_ID=kylin_account.ACCOUNT_ID group by name",
                "select schema1.table1.col1 from %s.schema1.table1" };
        String[] afterRemoveSql = new String[] {
                "select name, count(*) as cnt from schema1.user where bb.dd >2 group by name",
                "select name, count(*) as cnt from .default2.user where dd >2 group by name",
                "select name, count(*) as cnt from default2.user where dd >2 group by name",
                "select name, count(*) as cnt from user.a.cu where dd >2 group by name",
                "select name, count(*) as cnt from default2.user where dd >2 group by name",
                "select name, count() as cnt from test.kylin_sales inner join " + "test.kylin_account "
                        + "ON kylin_sales.BUYER_ID=kylin_account.ACCOUNT_ID group by name",
                "select schema1.table1.col1 from schema1.table1" };
        Assert.assertEquals(afterRemoveSql.length, beforeRemoveSql.length);
        for (int i = 0; i < beforeRemoveSql.length; i++) {
            String before = beforeRemoveSql[i];
            before = before.replace("%s", catalog);
            String after = afterRemoveSql[i];
            Assert.assertEquals(after, QueryUtil.removeCatalog(before, catalog));
        }
    }

    @Test
    public void testLimitAndOffset() {
        String sql = "select e_name from employees_china union select e_name from employees_usa";
        String result1 = QueryUtil.massageSql(sql, "default", 0, 0, "DEFAULT");
        Assert.assertEquals(result1, "select e_name from employees_china union " +
                "select e_name from employees_usa");

        String result2 = QueryUtil.massageSql(sql, "default", 10, 0, "DEFAULT");
        Assert.assertEquals(result2, "select * from (select e_name from employees_china union " +
                "select e_name from employees_usa) limit 10");

        String result3 = QueryUtil.massageSql(sql, "default", 0, 5, "DEFAULT");
        Assert.assertEquals(result3, "select * from (select e_name from employees_china union " +
                "select e_name from employees_usa) offset 5");

        String result4 = QueryUtil.massageSql(sql, "default", 10, 5, "DEFAULT");
        Assert.assertEquals(result4, "select * from (select e_name from employees_china union " +
                "select e_name from employees_usa) limit 10 offset 5");
    }
}

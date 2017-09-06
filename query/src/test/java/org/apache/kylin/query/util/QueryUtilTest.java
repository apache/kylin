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

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueryUtilTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testMassageSql() {
        {
            String sql = "select ( date '2001-09-28' + interval floor(1.2) day) from test_kylin_fact";
            String s = QueryUtil.massageSql(sql, null, 0, 0, "DEFAULT");
            Assert.assertEquals("select ( date '2001-09-28' + interval '1' day) from test_kylin_fact", s);
        }
        {
            String sql = "select ( date '2001-09-28' + interval floor(2) month) from test_kylin_fact group by ( date '2001-09-28' + interval floor(2) month)";
            String s = QueryUtil.massageSql(sql, null, 0, 0, "DEFAULT");
            Assert.assertEquals(
                    "select ( date '2001-09-28' + interval '2' month) from test_kylin_fact group by ( date '2001-09-28' + interval '2' month)",
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
    }

    @Test
    public void testKeywordDefaultDirtyHack() {
        {
            String sql = "select * from DEFAULT.TEST_KYLIN_FACT";
            String s = QueryUtil.massageSql(sql, null, 0, 0, "DEFAULT");
            Assert.assertEquals("select * from \"DEFAULT\".TEST_KYLIN_FACT", s);
        }
    }

    @Test
    public void testRemoveCommentInSql() {

        String originSql = "select count(*) from test_kylin_fact where price > 10.0";

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

    }
}

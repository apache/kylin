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

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class WhiteSpaceParserTest {

    private final EscapeDialect dialect = EscapeDialect.DEFAULT;

    private static final String expected = "select ACCOUNT_ID, ACCOUNT_ID + 1, 'a     +    b', 'c* d', ACCOUNT_ID + 2, count ( * ) from KYLIN_ACCOUNT group by ACCOUNT_ID limit 400 ";

    private static final String realSqlWithBinary = "select ACCOUNT_ID, ACCOUNT_ID + 1, x'a+ b', X'c *     d', ACCOUNT_ID + 2, count ( * ) from KYLIN_ACCOUNT group by ACCOUNT_ID limit 400 ";

    private static final String realSql = "select ACCOUNT_ID, ACCOUNT_ID + 1, 'a+ b', 'c * d', ACCOUNT_ID + 2, count ( * ) from KYLIN_ACCOUNT group by ACCOUNT_ID limit 400 ";

    @Test
    public void testMultiWhiteSpaces() throws ParseException {
        String sql = "select   ACCOUNT_ID, ACCOUNT_ID + 1 ,'a     +    b'  ,'c* d', ACCOUNT_ID +2,count(*) from KYLIN_ACCOUNT group  by  ACCOUNT_ID  limit  400   ";
        WhiteSpaceParser whiteSpaceParser = new WhiteSpaceParser(dialect, sql.trim());
        String parsed = whiteSpaceParser.parse();
        Assert.assertEquals(expected, parsed);
    }

    @Test
    public void testNewLines() throws ParseException {
        String newLines = "select   ACCOUNT_ID, ACCOUNT_ID + 1 ,'a     +    b'  ,'c* d', ACCOUNT_ID +2,count(*) from "
                + "KYLIN_ACCOUNT " + "group   by  ACCOUNT_ID  " + "limit  400   ";
        testEqual(newLines);
    }

    @Test
    public void testCommas() throws ParseException {
        String commas = "select   ACCOUNT_ID   ,     ACCOUNT_ID + 1  ,  'a     +    b'  ,'c* d', ACCOUNT_ID +2,count(*) from "
                + "KYLIN_ACCOUNT " + "group   by  ACCOUNT_ID  " + "limit  400   ";
        testEqual(commas);
    }

    @Test
    public void testOperators() throws ParseException {
        String operators = "select   ACCOUNT_ID   ,     ACCOUNT_ID     +    1  ,  'a     +    b'  ,'c* d', ACCOUNT_ID+2,count(*) from "
                + "KYLIN_ACCOUNT " + "group   by  ACCOUNT_ID  " + "limit  400   ";
        testEqual(operators);
    }

    private void testEqual(String sql) throws ParseException {
        WhiteSpaceParser whiteSpaceParser = new WhiteSpaceParser(dialect, sql.trim());
        String parsed = whiteSpaceParser.parse();
        Assert.assertEquals(expected, parsed);
    }

    @Test
    public void testNormalEscapeString() throws ParseException {
        String sql = "select   ACCOUNT_ID   ,     ACCOUNT_ID     +    1  ,  'a+ b'  ,'c * d', ACCOUNT_ID+2,count(*) from "
                + "KYLIN_ACCOUNT " + "group   by  ACCOUNT_ID  " + "limit  400   ";
        WhiteSpaceParser whiteSpaceParser = new WhiteSpaceParser(dialect, sql.trim());
        String parsed = whiteSpaceParser.parse();
        Assert.assertNotEquals(expected, parsed);
        Assert.assertEquals(realSql, parsed);
    }

    @Test
    public void testBinaryEscapeStringWithRegexWhiteSpace() throws ParseException {
        String sql = "select   ACCOUNT_ID   ,     ACCOUNT_ID     +    1  ,  x'a+ b'  ,X'c *     d', ACCOUNT_ID+2,count(*) from "
                + "KYLIN_ACCOUNT " + "group   by  ACCOUNT_ID  " + "limit  400   ";
        String sql1 = "select   ACCOUNT_ID   ,  \n   ACCOUNT_ID   \r  +    1  ,  x'a+ b'  ,X'c *     d',\f ACCOUNT_ID+2,count(*) from "
                + "KYLIN_ACCOUNT " + "group   by  ACCOUNT_ID  " + "limit  400   ";
        String sql2 = "select   ACCOUNT_ID   ,  \n\n   ACCOUNT_ID   \r\r  +    1  ,  x'a+ b'  ,X'c *     d',\f ACCOUNT_ID+2,count(*) from "
                + "KYLIN_ACCOUNT " + "group   by  ACCOUNT_ID  " + "limit  400   ";
        String sql3 = "select \n  ACCOUNT_ID   ,  \n\n\r\t   ACCOUNT_ID   \r\r  +    1  ,  x'a+ b'  ,X'c *     d',\f ACCOUNT_ID+2,count(*) from "
                + "KYLIN_ACCOUNT " + "group   by  ACCOUNT_ID  " + "limit  400   ";
        testBinary(sql);
        testBinary(sql1);
        testBinary(sql2);
        testBinary(sql3);
    }

    @Test
    public void testExcessiveExpressionNestingRejected() throws ParseException {
        String nestedSql = nestedExpression(300);

        ParseException whitespaceException = Assert.assertThrows(ParseException.class,
                () -> new WhiteSpaceParser(dialect, nestedSql).parse());
        Assert.assertTrue(whitespaceException.getMessage().contains("nesting exceeds"));

        ParseException escapeException = Assert.assertThrows(ParseException.class,
                () -> new EscapeParser(dialect, nestedSql).Input());
        Assert.assertTrue(escapeException.getMessage().contains("nesting exceeds"));

        String shallowSql = nestedExpression(64);
        Assert.assertNotNull(new WhiteSpaceParser(dialect, shallowSql).parse());
        Assert.assertNotNull(new EscapeParser(dialect, shallowSql).Input());
    }

    private String nestedExpression(int depth) {
        return "select " + StringUtils.repeat("(", depth) + "1=1" + StringUtils.repeat(")", depth);
    }

    private void testBinary(String sql) throws ParseException {
        WhiteSpaceParser whiteSpaceParser = new WhiteSpaceParser(dialect, sql.trim());
        String parsed = whiteSpaceParser.parse();
        Assert.assertNotEquals(expected, parsed);
        Assert.assertEquals(realSqlWithBinary, parsed);
    }

    @Test
    public void testExtractEpoch() throws ParseException {
        String sql = "SELECT ((EXTRACT(EPOCH FROM (DATE '2050-01-01')) / ( 60 * 60 * 24)) + (365 * 70 + 17)) AS \"TEMP(Test)(3947742720)(0)\" FROM \"TDVT\".\"CALCS\" \"CALCS\" HAVING (COUNT(1) > 0)";
        WhiteSpaceParser whiteSpaceParser = new WhiteSpaceParser(dialect, sql.trim());
        String parsed = whiteSpaceParser.parse();
        String real = "SELECT (({fn EPOCH((DATE '2050-01-01' ))} /  (60 *  60 *  24 ) ) +  (365 *  70 +  17 ) ) AS \"TEMP(Test)(3947742720)(0)\" FROM \"TDVT\" . \"CALCS\" \"CALCS\" HAVING (COUNT (1 ) > 0 ) ";
        Assert.assertEquals(real, parsed);
    }
}

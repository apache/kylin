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
package org.apache.kylin.sdk.datasource.framework.conv;

import java.util.Map;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Assert;
import org.junit.Test;

public class SqlParamsFinderTest {

    @Test
    public void testParamFinder() throws SqlParseException {
        SqlParser sqlParser1 = SqlParser.create("POWER($0, $1) + AVG(LN($3)) + EXP($5)");
        SqlNode sqlPattern = sqlParser1.parseExpression();
        SqlParser sqlParser2 = SqlParser
                .create("POWER(3, POWER(2, POWER(2, 3))) + AVG(LN(EXP(4))) + EXP(CAST('2018-03-22' AS DATE))");
        SqlNode sqlCall = sqlParser2.parseExpression();

        SqlParamsFinder sqlParamsFinder = new SqlParamsFinder((SqlCall) sqlPattern, (SqlCall) sqlCall);
        Map<Integer, SqlNode> paramNodes = sqlParamsFinder.getParamNodes();

        Assert.assertEquals("3", paramNodes.get(0).toString());
        Assert.assertEquals("POWER(2, POWER(2, 3))", paramNodes.get(1).toString());
        Assert.assertEquals("EXP(4)", paramNodes.get(3).toString());
        Assert.assertEquals("CAST('2018-03-22' AS DATE)", paramNodes.get(5).toString());

    }

    @Test
    public void testWindowCallParams() throws SqlParseException {
        SqlParser sqlParser1 = SqlParser.create("STDDEV_POP($0) OVER($1)");
        SqlNode sqlPattern = sqlParser1.parseExpression();
        SqlParser sqlParser2 = SqlParser.create("STDDEV_POP(C1) OVER (ORDER BY C1)");
        SqlNode sqlCall = sqlParser2.parseExpression();
        SqlParamsFinder sqlParamsFinder = SqlParamsFinder.newInstance((SqlCall) sqlPattern, (SqlCall) sqlCall, true);
        Map<Integer, SqlNode> paramNodes = sqlParamsFinder.getParamNodes();

        Assert.assertEquals("C1", paramNodes.get(0).toString());
        Assert.assertEquals("(ORDER BY `C1`)", paramNodes.get(1).toString());
        Assert.assertTrue(paramNodes.get(1) instanceof SqlWindow);
    }
}

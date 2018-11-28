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

package org.apache.kylin.job.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class FlatTableSqlQuoteUtilsTest {

    @Test
    public void testQuoteTableName() {
        List<String> tablePatterns = FlatTableSqlQuoteUtils.getTableNameOrAliasPatterns("KYLIN_SALES");
        String expr = "KYLIN_SALES.PRICE * KYLIN_SALES.COUNT";
        String expectedExpr = "`KYLIN_SALES`.PRICE * `KYLIN_SALES`.COUNT";
        String quotedExpr = FlatTableSqlQuoteUtils.quoteIdentifier(expr, "`", "KYLIN_SALES", tablePatterns);
        Assert.assertEquals(expectedExpr, quotedExpr);

        expr = "`KYLIN_SALES`.PRICE * KYLIN_SALES.COUNT";
        expectedExpr = "`KYLIN_SALES`.PRICE * `KYLIN_SALES`.COUNT";
        quotedExpr = FlatTableSqlQuoteUtils.quoteIdentifier(expr, "`", "KYLIN_SALES", tablePatterns);
        Assert.assertEquals(expectedExpr, quotedExpr);

        expr = "KYLIN_SALES.PRICE AS KYLIN_SALES_PRICE * KYLIN_SALES.COUNT AS KYLIN_SALES_COUNT";
        expectedExpr = "`KYLIN_SALES`.PRICE AS KYLIN_SALES_PRICE * `KYLIN_SALES`.COUNT AS KYLIN_SALES_COUNT";
        quotedExpr = FlatTableSqlQuoteUtils.quoteIdentifier(expr, "`", "KYLIN_SALES", tablePatterns);
        Assert.assertEquals(expectedExpr, quotedExpr);

        expr = "(KYLIN_SALES.PRICE AS KYLIN_SALES_PRICE > 1 and KYLIN_SALES.COUNT AS KYLIN_SALES_COUNT > 50)";
        expectedExpr = "(`KYLIN_SALES`.PRICE AS KYLIN_SALES_PRICE > 1 and `KYLIN_SALES`.COUNT AS KYLIN_SALES_COUNT > 50)";
        quotedExpr = FlatTableSqlQuoteUtils.quoteIdentifier(expr, "`", "KYLIN_SALES", tablePatterns);
        Assert.assertEquals(expectedExpr, quotedExpr);
    }

    @Test
    public void testQuoteTableAliasName() {
        List<String> tablePatterns = FlatTableSqlQuoteUtils.getTableNameOrAliasPatterns("KYLIN_SALES_ALIAS");
        String expr = "KYLIN_SALES.PRICE * KYLIN_SALES.COUNT";
        String expectedExpr = "KYLIN_SALES.PRICE * KYLIN_SALES.COUNT";
        String quotedExpr = FlatTableSqlQuoteUtils.quoteIdentifier(expr, "`", "KYLIN_SALES_ALIAS", tablePatterns);
        Assert.assertEquals(expectedExpr, quotedExpr);

        expr = "KYLIN_SALES.PRICE AS KYLIN_SALES_PRICE * KYLIN_SALES.COUNT AS KYLIN_SALES_COUNT";
        expectedExpr = "KYLIN_SALES.PRICE AS KYLIN_SALES_PRICE * KYLIN_SALES.COUNT AS KYLIN_SALES_COUNT";
        quotedExpr = FlatTableSqlQuoteUtils.quoteIdentifier(expr, "`", "KYLIN_SALES_ALIAS", tablePatterns);
        Assert.assertEquals(expectedExpr, quotedExpr);

        expr = "(KYLIN_SALES.PRICE AS KYLIN_SALES_PRICE > 1 and KYLIN_SALES.COUNT AS KYLIN_SALES_COUNT > 50)";
        expectedExpr = "(KYLIN_SALES.PRICE AS KYLIN_SALES_PRICE > 1 and KYLIN_SALES.COUNT AS KYLIN_SALES_COUNT > 50)";
        quotedExpr = FlatTableSqlQuoteUtils.quoteIdentifier(expr, "`", "KYLIN_SALES_ALIAS", tablePatterns);
        Assert.assertEquals(expectedExpr, quotedExpr);

        expr = "(KYLIN_SALES_ALIAS.PRICE AS KYLIN_SALES_PRICE > 1 and KYLIN_SALES.COUNT AS KYLIN_SALES_COUNT > 50)";
        expectedExpr = "(`KYLIN_SALES_ALIAS`.PRICE AS KYLIN_SALES_PRICE > 1 and KYLIN_SALES.COUNT AS KYLIN_SALES_COUNT > 50)";
        quotedExpr = FlatTableSqlQuoteUtils.quoteIdentifier(expr, "`", "KYLIN_SALES_ALIAS", tablePatterns);
        Assert.assertEquals(expectedExpr, quotedExpr);
    }

    @Test
    public void testQuoteColumnName() {
        List<String> columnPatterns = FlatTableSqlQuoteUtils.getColumnNameOrAliasPatterns("PRICE");
        String expr = "KYLIN_SALES.PRICE * KYLIN_SALES.COUNT";
        String expectedExpr = "KYLIN_SALES.`PRICE` * KYLIN_SALES.COUNT";
        String quotedExpr = FlatTableSqlQuoteUtils.quoteIdentifier(expr, "`", "PRICE", columnPatterns);
        Assert.assertEquals(expectedExpr, quotedExpr);

        expr = "KYLIN_SALES.PRICE/KYLIN_SALES.COUNT";
        expectedExpr = "KYLIN_SALES.`PRICE`/KYLIN_SALES.COUNT";
        quotedExpr = FlatTableSqlQuoteUtils.quoteIdentifier(expr, "`", "PRICE", columnPatterns);
        Assert.assertEquals(expectedExpr, quotedExpr);

        expr = "KYLIN_SALES.PRICE AS KYLIN_SALES_PRICE * KYLIN_SALES.COUNT AS KYLIN_SALES_COUNT";
        expectedExpr = "KYLIN_SALES.`PRICE` AS KYLIN_SALES_PRICE * KYLIN_SALES.COUNT AS KYLIN_SALES_COUNT";
        quotedExpr = FlatTableSqlQuoteUtils.quoteIdentifier(expr, "`", "PRICE", columnPatterns);
        Assert.assertEquals(expectedExpr, quotedExpr);

        expr = "(PRICE > 1 AND COUNT > 50)";
        expectedExpr = "(`PRICE` > 1 AND COUNT > 50)";
        quotedExpr = FlatTableSqlQuoteUtils.quoteIdentifier(expr, "`", "PRICE", columnPatterns);
        Assert.assertEquals(expectedExpr, quotedExpr);

        expr = "PRICE>1 and `PRICE` < 15";
        expectedExpr = "`PRICE`>1 and `PRICE` < 15";
        quotedExpr = FlatTableSqlQuoteUtils.quoteIdentifier(expr, "`", "PRICE", columnPatterns);
        Assert.assertEquals(expectedExpr, quotedExpr);
    }

    @Test
    public void testIsTableNameOrAliasNeedToQuote() {
        List<String> tablePatterns = FlatTableSqlQuoteUtils.getTableNameOrAliasPatterns("kylin_sales");
        Assert.assertTrue(FlatTableSqlQuoteUtils.isIdentifierNeedToQuote("KYLIN_SALES.PRICE * KYLIN_SALES.COUNT",
                "kylin_sales", tablePatterns));
        Assert.assertTrue(FlatTableSqlQuoteUtils.isIdentifierNeedToQuote("KYLIN_SALES.PRICE*KYLIN_SALES.COUNT",
                "kylin_sales", tablePatterns));
        Assert.assertTrue(FlatTableSqlQuoteUtils.isIdentifierNeedToQuote(
                "KYLIN_SALES.PRICE AS KYLIN_SALES_PRICE * KYLIN_SALES.COUNT AS KYLIN_SALES_COUNT", "kylin_sales",
                tablePatterns));
        Assert.assertTrue(
                FlatTableSqlQuoteUtils.isIdentifierNeedToQuote("KYLIN_SALES.PRICE>1", "kylin_sales", tablePatterns));
        Assert.assertTrue(FlatTableSqlQuoteUtils.isIdentifierNeedToQuote("(KYLIN_SALES.PRICE * KYLIN_SALES.COUNT)",
                "kylin_sales", tablePatterns));
        Assert.assertTrue(FlatTableSqlQuoteUtils.isIdentifierNeedToQuote(
                "`KYLIN_SALES`.PRICE AS KYLIN_SALES_PRICE * KYLIN_SALES.COUNT AS KYLIN_SALES_COUNT", "kylin_sales",
                tablePatterns));

        Assert.assertFalse(FlatTableSqlQuoteUtils.isIdentifierNeedToQuote("`KYLIN_SALES`.PRICE * `KYLIN_SALES`.COUNT",
                "kylin_sales", tablePatterns));
        Assert.assertFalse(FlatTableSqlQuoteUtils.isIdentifierNeedToQuote(
                "\"KYLIN_SALES\".PRICE * \"KYLIN_SALES\".COUNT", "kylin_sales", tablePatterns));
        Assert.assertFalse(FlatTableSqlQuoteUtils.isIdentifierNeedToQuote(
                "\'KYLIN_SALES\'.PRICE * \'KYLIN_SALES\'.COUNT", "kylin_sales", tablePatterns));
        Assert.assertFalse(FlatTableSqlQuoteUtils.isIdentifierNeedToQuote("KYLIN_SALES_PRICE * KYLIN_SALES_COUNT",
                "kylin_sales", tablePatterns));
    }
}
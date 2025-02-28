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

package org.apache.kylin.rec.query;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.rec.query.advisor.SQLAdvice;
import org.apache.kylin.rec.query.validator.SQLValidateResult;
import org.apache.kylin.rec.query.validator.SqlSyntaxValidator;
import org.junit.Assert;
import org.junit.Test;

public class SqlSyntaxValidatorTest extends SqlValidateTestBase {

    @Test
    public void testGoodCases() {

        String[] goodSqls = new String[] { //
                "select 1",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = {d '2012-01-01'} group by part_dt, lstg_format_name",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where lstg_format_name > 'ABIN' group by part_dt, lstg_format_name",
                "select part_dt, sum(item_count), count(*) from kylin_sales group by part_dt",
                "select part_dt, sum(item_count) from kylin_sales group by part_dt" };

        SqlSyntaxValidator validator = new SqlSyntaxValidator(proj, getTestConfig());
        final Map<String, SQLValidateResult> goodResults = validator.batchValidate(goodSqls);
        printSqlValidateResults(goodResults);
        goodResults.forEach((key, sqlValidateResult) -> Assert.assertTrue(sqlValidateResult.isCapable()));
    }

    @Test
    public void testBadCases() {

        // see https://olapio.atlassian.net/browse/KE-42034
        String[] badSqls = new String[] { //
                "create table a", // not select statement
                "select columnA, price from kylin_sales", // 'columnA' not found
                "select price from kylin_sales group by" // incomplete sql
                // Calcite 1.30 supports this syntax
                // "select sum(lstg_format_name) from kylin_sales" // can not apply sum to 'lstg_format_name'
        };

        SqlSyntaxValidator validator = new SqlSyntaxValidator(proj, getTestConfig());
        final Map<String, SQLValidateResult> badResults = validator.batchValidate(badSqls);
        printSqlValidateResults(badResults);
        badResults.forEach((key, sqlValidateResult) -> Assert.assertFalse(sqlValidateResult.isCapable()));

        String[] badSqls2 = new String[] { "select columnA, price from kylin_sales" };
        final Map<String, SQLValidateResult> badResults2 = validator.batchValidate(badSqls2);
        badResults2.forEach((key, sqlValidateResult) -> {
            Set<SQLAdvice> sqlAdvices = sqlValidateResult.getSqlAdvices();
            sqlAdvices.forEach(sqlAdvice -> {
                String colName = "columnA".toUpperCase(Locale.ROOT);
                Assert.assertEquals(
                        String.format(Locale.ROOT, MsgPicker.getMsg().getBadSqlColumnNotFoundReason(), colName),
                        sqlAdvice.getIncapableReason());
                Assert.assertEquals(
                        String.format(Locale.ROOT, MsgPicker.getMsg().getBadSqlColumnNotFoundReason(), colName),
                        sqlAdvice.getSuggestion());
            });
        });
    }
}

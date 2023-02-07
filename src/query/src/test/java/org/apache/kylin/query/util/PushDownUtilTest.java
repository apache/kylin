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

import java.util.Properties;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PushDownUtilTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testTryForcePushDown() {
        try {
            QueryParams queryParams = new QueryParams();
            queryParams.setProject("default");
            queryParams.setSelect(true);
            queryParams.setForcedToPushDown(true);
            PushDownUtil.tryIterQuery(queryParams);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertFalse(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testTryWithPushDownDisable() {
        try {
            overwriteSystemProp("kylin.query.pushdown-enabled", "false");
            QueryParams queryParams = new QueryParams();
            queryParams.setProject("default");
            queryParams.setSelect(true);
            queryParams.setForcedToPushDown(true);
            PushDownUtil.tryIterQuery(queryParams);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertFalse(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testBacktickQuote() {
        String table = "db.table";
        Assert.assertEquals("`db`.`table`", String.join(".", PushDownUtil.backtickQuote(table.split("\\."))));
    }

    @Test
    public void testMassagePushDownSql() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.pushdown.converter-class-names",
                    SparkSQLFunctionConverter.class.getCanonicalName());
            String sql = "SELECT \"Z_PROVDASH_UM_ED\".\"GENDER\" AS \"GENDER\",\n"
                    + "SUM({fn CONVERT(0, SQL_BIGINT)}) AS \"sum_Calculation_336925569152049156_ok\"\n"
                    + "FROM \"POPHEALTH_ANALYTICS\".\"Z_PROVDASH_UM_ED\" \"Z_PROVDASH_UM_ED\"";

            QueryParams queryParams = new QueryParams("", sql, "default", false);
            queryParams.setKylinConfig(config);
            String massagedSql = PushDownUtil.massagePushDownSql(queryParams);
            String expectedSql = "SELECT `Z_PROVDASH_UM_ED`.`GENDER` AS `GENDER`,\n"
                    + "SUM(CAST(0 AS BIGINT)) AS `sum_Calculation_336925569152049156_ok`\n"
                    + "FROM `POPHEALTH_ANALYTICS`.`Z_PROVDASH_UM_ED` `Z_PROVDASH_UM_ED`";
            Assert.assertEquals(expectedSql, massagedSql);
        }
    }

    @Test
    public void testMassagePushDownSqlWithDoubleQuote() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        String sql = "select '''',trans_id from test_kylin_fact where LSTG_FORMAT_NAME like '%''%' group by trans_id limit 2;";
        QueryParams queryParams = new QueryParams("", sql, "default", false);
        queryParams.setKylinConfig(config);
        String massagedSql = PushDownUtil.massagePushDownSql(queryParams);
        String expectedSql = "select '\\'', `TRANS_ID` from `TEST_KYLIN_FACT` where `LSTG_FORMAT_NAME` like '%\\'%' group by `TRANS_ID` limit 2";
        Assert.assertEquals(expectedSql, massagedSql);
    }

    @Test
    public void testMassagePushDownSqlWithDialectConverter() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.pushdown.converter-class-names",
                    "org.apache.kylin.query.util.DialectConverter,org.apache.kylin.source.adhocquery.DoubleQuotePushDownConverter,"
                            + SparkSQLFunctionConverter.class.getCanonicalName());
            String sql = "SELECT \"Z_PROVDASH_UM_ED\".\"GENDER\" AS \"GENDER\",\n"
                    + "SUM({fn CONVERT(0, SQL_BIGINT)}) AS \"sum_Calculation_336925569152049156_ok\"\n"
                    + "FROM \"POPHEALTH_ANALYTICS\".\"Z_PROVDASH_UM_ED\" \"Z_PROVDASH_UM_ED\""
                    + " fetch first 1 rows only";

            QueryParams queryParams = new QueryParams("", sql, "default", false);
            queryParams.setKylinConfig(config);
            String massagedSql = PushDownUtil.massagePushDownSql(queryParams);
            String expectedSql = "SELECT `Z_PROVDASH_UM_ED`.`GENDER` AS `GENDER`, "
                    + "SUM(CAST(0 AS BIGINT)) AS `sum_Calculation_336925569152049156_ok`\n"
                    + "FROM `POPHEALTH_ANALYTICS`.`Z_PROVDASH_UM_ED` AS `Z_PROVDASH_UM_ED`\n" + "LIMIT 1";
            Assert.assertEquals(expectedSql, massagedSql);
        }
    }

    @Test
    public void testReplaceDoubleQuoteToSingle() {
        String sql = "select ab from table where aa = '' and bb = '''as''n'''";
        String resSql = "select ab from table where aa = '' and bb = '\\'as\\'n\\''";
        Assert.assertEquals(resSql, PushDownUtil.replaceEscapedQuote(sql));
    }
}

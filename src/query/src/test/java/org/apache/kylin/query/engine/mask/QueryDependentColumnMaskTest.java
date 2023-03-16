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

package org.apache.kylin.query.engine.mask;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.acl.DependentColumn;
import org.apache.kylin.metadata.acl.DependentColumnInfo;
import org.apache.kylin.query.QueryExtension;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.mask.QueryDependentColumnMask;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

public class QueryDependentColumnMaskTest extends NLocalFileMetadataTestCase {

    private QueryDependentColumnMask mask = null;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        DependentColumnInfo dependentColumnInfo = new DependentColumnInfo();
        dependentColumnInfo.add("DEFAULT", "TEST_KYLIN_FACT", Lists.newArrayList(
                new DependentColumn("PRICE", "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL", new String[] { "1", "2" })));
        dependentColumnInfo.add("DEFAULT", "TEST_KYLIN_FACT",
                Lists.newArrayList(
                        new DependentColumn("ORDER_ID", "DEFAULT.TEST_ACCOUNT.ACCOUNT_SELLER_LEVEL",
                                new String[] { "1", "2" }),
                        new DependentColumn("ORDER_ID", "DEFAULT.TEST_COUNTRY.NAME", new String[] { "China" })));

        dependentColumnInfo.add("DEFAULT", "TEST_MEASURE", Lists.newArrayList(
                new DependentColumn("ID1", "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL", new String[] { "1", "2" }),
                new DependentColumn("ID4", "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL", new String[] { "1", "2" }),
                new DependentColumn("PRICE1", "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL", new String[] { "1", "2" }),
                new DependentColumn("PRICE2", "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL", new String[] { "1", "2" }),
                new DependentColumn("PRICE3", "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL", new String[] { "1", "2" }),
                new DependentColumn("PRICE5", "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL", new String[] { "1", "2" }),
                new DependentColumn("PRICE6", "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL", new String[] { "1", "2" }),
                new DependentColumn("PRICE7", "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL", new String[] { "1", "2" }),
                new DependentColumn("NAME1", "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL", new String[] { "1", "2" }),
                new DependentColumn("NAME2", "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL", new String[] { "1", "2" }),
                new DependentColumn("TIME1", "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL", new String[] { "1", "2" }),
                new DependentColumn("TIME2", "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL", new String[] { "1", "2" }),
                new DependentColumn("FLAG", "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL", new String[] { "1", "2" })));
        mask = new QueryDependentColumnMask("DEFAULT", dependentColumnInfo);
        // Use default Factory for Open Core
        QueryExtension.setFactory(new QueryExtension.Factory());
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
        // Unset Factory for Open Core
        QueryExtension.setFactory(null);
    }

    @Test
    public void testSetMultiMask() {
        DependentColumnInfo dependentColumnInfo = new DependentColumnInfo();
        dependentColumnInfo.add("DEFAULT", "TEST_KYLIN_FACT1", Lists.newArrayList(
                new DependentColumn("PRICE", "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL", new String[] { "1" })));
        dependentColumnInfo.add("DEFAULT", "TEST_KYLIN_FACT1", Lists.newArrayList(
                new DependentColumn("PRICE", "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL", new String[] { "2", "3" })));
        Assert.assertEquals(1, dependentColumnInfo.get("DEFAULT.TEST_KYLIN_FACT1.PRICE").size());
        String[] values = dependentColumnInfo.get("DEFAULT.TEST_KYLIN_FACT1.PRICE").iterator().next()
                .getDependentValues();
        Arrays.sort(values);
        Assert.assertArrayEquals(new String[] { "1", "2", "3" }, values);
    }

    @Test
    public void testSimpleMask() {
        {
            String sql = "SELECT ACCOUNT_BUYER_LEVEL, ID1, ID4, PRICE1, PRICE2, PRICE3, PRICE5, PRICE6, PRICE7, NAME1, NAME2, TIME1, TIME2, FLAG"
                    + " FROM TEST_MEASURE join TEST_ACCOUNT on ACCOUNT_ID = ID1";
            String[] before = new String[] { "1", "1", "123", "1.2", "2.3", "3.4", "1", "2", "3", "CN", "FOO",
                    "1992-01-01", "1992-01-01 00:10:12", "true" };
            testMask(sql, before, before);

            String[] before1 = new String[] { "3", "1", "123", "1.2", "2.3", "3.4", "1", "2", "3", "CN", "FOO",
                    "1992-01-01", "1992-01-01 00:10:12", "true" };
            String[] expected = new String[] { "3", null, null, null, null, null, null, null, null, null, null, null,
                    null, null };
            testMask(sql, before1, expected);
        }

        {
            String sql = "SELECT ID1, ID4, PRICE1, PRICE2, PRICE3, PRICE5, PRICE6, PRICE7, NAME1, NAME2, TIME1, TIME2, FLAG FROM TEST_MEASURE";
            String[] before = new String[] { "1", "123", "1.2", "2.3", "3.4", "1", "2", "3", "CN", "FOO", "1992-01-01",
                    "1992-01-01 00:10:12", "true" };
            String[] expected = new String[] { null, null, null, null, null, null, null, null, null, null, null, null,
                    null };
            testMask(sql, before, expected);
        }
    }

    @Test
    public void testDependentColMissing() {
        // with calc
        {
            String sql = "SELECT PRICE, ACCOUNT_BUYER_LEVEL + 1"
                    + " FROM TEST_KYLIN_FACT join TEST_ACCOUNT on SELLER_ID = ACCOUNT_ID";
            String[] before = new String[] { "1", "1" };
            String[] expected = new String[] { null, "1" };
            testMask(sql, before, expected);
        }

        // with agg
        {
            String sql = "SELECT PRICE, MAX(ACCOUNT_BUYER_LEVEL)"
                    + " FROM TEST_KYLIN_FACT join TEST_ACCOUNT on SELLER_ID = ACCOUNT_ID GROUP BY PRICE";
            String[] before = new String[] { "1", "1" };
            String[] expected = new String[] { null, "1" };
            testMask(sql, before, expected);
        }

        // with multiple dependent cols
        {
            String sql = "SELECT ORDER_ID, ACCOUNT_SELLER_LEVEL + 1, NAME"
                    + " FROM TEST_KYLIN_FACT join TEST_ACCOUNT on SELLER_ID = ACCOUNT_ID"
                    + " join TEST_COUNTRY on ACCOUNT_COUNTRY = NAME";
            String[] before = new String[] { "1", "1", "1" };
            String[] expected = new String[] { null, "1", "1" };
            testMask(sql, before, expected);
        }
    }

    @Test
    public void testAgg() {
        String sql = "SELECT MAX(PRICE+1)+1, ACCOUNT_BUYER_LEVEL"
                + " FROM TEST_KYLIN_FACT join TEST_ACCOUNT on SELLER_ID = ACCOUNT_ID GROUP BY ACCOUNT_BUYER_LEVEL";
        String[] before = new String[] { "1", "1" };
        testMask(sql, before, before);

        String[] before1 = new String[] { "1", "3" };
        String[] expected = new String[] { null, "3" };
        testMask(sql, before1, expected);
    }

    @Test
    public void testAggOnAgg() {
        String sql = "SELECT SUM(MAXPRICE), BUYERLEVEL AS LEVEL FROM "
                + "(SELECT MAX(PRICE+1)+1 AS MAXPRICE, ACCOUNT_BUYER_LEVEL AS BUYERLEVEL, ACCOUNT_ID"
                + " FROM TEST_KYLIN_FACT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID GROUP BY ACCOUNT_BUYER_LEVEL, ACCOUNT_ID)"
                + " WHERE ACCOUNT_ID > 1 GROUP BY BUYERLEVEL";
        String[] before = new String[] { "1", "1" };
        testMask(sql, before, before);

        String[] before1 = new String[] { "1", "3" };
        String[] expected = new String[] { null, "3" };
        testMask(sql, before1, expected);
    }

    @Test
    public void testMultiJoin() {
        String sql = "SELECT ORDER_ID, ACCOUNT_SELLER_LEVEL, NAME"
                + " FROM TEST_KYLIN_FACT join TEST_ACCOUNT on SELLER_ID = ACCOUNT_ID"
                + " join TEST_COUNTRY on ACCOUNT_COUNTRY = NAME";
        String[] before = new String[] { "1", "1", "China" };
        testMask(sql, before, before);

        String[] before1 = new String[] { "1", "1", "US" };
        String[] expected = new String[] { null, "1", "US" };
        testMask(sql, before1, expected);
    }

    @Test
    public void testJoinWithAgg() {
        String sql = "SELECT ORDER_ID, ACCOUNT_SELLER_LEVEL, NAME" + " FROM "
                + " (SELECT COUNT(1), ORDER_ID, ACCOUNT_SELLER_LEVEL, ACCOUNT_COUNTRY "
                + " FROM TEST_KYLIN_FACT LEFT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID"
                + " GROUP BY ORDER_ID, ACCOUNT_SELLER_LEVEL, ACCOUNT_COUNTRY)"
                + " JOIN TEST_COUNTRY ON ACCOUNT_COUNTRY = NAME";
        String[] before = new String[] { "1", "1", "China" };
        testMask(sql, before, before);

        String[] before1 = new String[] { "1", "1", "US" };
        String[] expected = new String[] { null, "1", "US" };
        testMask(sql, before1, expected);
    }

    @Test
    public void testValues() {
        String sql = "SELECT ID, COUNTRY, PRICE, ACCOUNT_BUYER_LEVEL "
                + " FROM (VALUES((1), ('US'))) AS V(ID, COUNTRY) JOIN TEST_KYLIN_FACT ON TRANS_ID = ID"
                + " JOIN TEST_ACCOUNT ON ACCOUNT_ID = SELLER_ID";
        String[] before = new String[] { "1", "CN", "12", "1" };
        testMask(sql, before, before);

        String[] before1 = new String[] { "1", "CN", "12", "3" };
        String[] expected = new String[] { "1", "CN", null, "3" };
        testMask(sql, before1, expected);
    }

    @Test
    public void testUnion() {
        String sql = "SELECT * FROM (\n" + "(SELECT PRICE, ACCOUNT_BUYER_LEVEL\n"
                + "FROM TEST_KYLIN_FACT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID\n"
                + "WHERE ACCOUNT_BUYER_LEVEL > 2)\n" + "UNION\n" + "(SELECT PRICE, ACCOUNT_BUYER_LEVEL\n"
                + "FROM TEST_KYLIN_FACT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID\n"
                + "WHERE ACCOUNT_BUYER_LEVEL = 1))\n";
        String[] before = new String[] { "1", "1" };
        testMask(sql, before, before);

        String[] before1 = new String[] { "1", "3" };
        String[] expected = new String[] { null, "3" };
        testMask(sql, before1, expected);
    }

    @Test
    public void testCC() {
        String sql = "SELECT SUM(DEAL_AMOUNT), SUM(NEST2), SUM(PRICE), ACCOUNT_BUYER_LEVEL"
                + " FROM TEST_KYLIN_FACT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID" + " GROUP BY ACCOUNT_BUYER_LEVEL";
        String[] before = new String[] { "1", "1", "1", "1" };
        testMask(sql, before, before);

        String[] before1 = new String[] { "1", "1", "1", "4" };
        String[] expected = new String[] { null, null, null, "4" };
        testMask(sql, before1, expected);
    }

    @Test
    public void testWindow() throws SqlParseException {
        String sql = "SELECT SUM(PRICE) OVER (PARTITION BY SELLER_ID ORDER BY TRANS_ID) AS ROW_NUM, "
                + "COUNT(1) OVER (PARTITION BY CAL_DT ORDER BY TRANS_ID) AS ROW_NUM, ACCOUNT_BUYER_LEVEL "
                + " FROM TEST_KYLIN_FACT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID";

        String[] before = new String[] { "1", "1", "1" };
        testMask(sql, before, before);

        String[] before1 = new String[] { "1", "1", "4" };
        String[] expected = new String[] { null, "1", "4" };
        testMask(sql, before1, expected);
    }

    @Test
    public void testSetEmptyMask() {
        DependentColumnInfo dependentColumnInfo = new DependentColumnInfo();
        dependentColumnInfo.add("DEFAULT", "TEST_KYLIN_FACT1", Lists.newArrayList());
        Assert.assertFalse(dependentColumnInfo.needMask());
    }

    private void testMask(String sql, String[] before, String[] expected) {
        QueryExec queryExec = new QueryExec("default", KylinConfig.getInstanceFromEnv());
        RelNode relNode = null;
        try {
            relNode = queryExec.parseAndOptimize(sql);
        } catch (SqlParseException e) {
            Assert.fail(e.getMessage());
        }

        mask.doSetRootRelNode(relNode);
        mask.init();
        Assert.assertArrayEquals(expected, doMaskResult(before, mask.getResultColumnMaskInfos()));
    }

    private String[] doMaskResult(String[] rowValues,
            List<QueryDependentColumnMask.ResultColumnMaskInfo> resultColumnMaskInfos) {
        String[] masked = new String[rowValues.length];
        for (int i = 0; i < rowValues.length; i++) {

            masked[i] = rowValues[i];
            QueryDependentColumnMask.ResultColumnMaskInfo maskInfo = resultColumnMaskInfos.get(i);
            if (!maskInfo.needMask()) {
                continue;
            }
            if (maskInfo.maskAsNull) {
                masked[i] = null;
                continue;
            }

            for (QueryDependentColumnMask.ResultDependentValues dependentValue : maskInfo.dependentValues) {
                String rowValue = rowValues[dependentValue.colIdx];
                if (!dependentValue.values.contains(rowValue)) {
                    masked[i] = null;
                    break;
                }
            }
        }
        return masked;
    }

}

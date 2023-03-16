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

package org.apache.kylin.query.security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.acl.AclTCR;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.query.exception.NoAuthorizedColsError;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

public class HackSelectStarWithColumnACLTest extends NLocalFileMetadataTestCase {
    private final static String PROJECT = "default";
    private final static String SCHEMA = "DEFAULT";

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "true");
    }

    @After
    public void after() {
        cleanupTestMetadata();
    }

    @Test
    public void testTransform() {
        prepareBasic();
        HackSelectStarWithColumnACL transformer = new HackSelectStarWithColumnACL();
        QueryContext.current().setAclInfo(new QueryContext.AclInfo("u1", Sets.newHashSet("g1"), false));
        String sql = transformer.convert(
                "select * from TEST_KYLIN_FACT t1 join TEST_ORDER t2 on t1.ORDER_ID = t2.ORDER_ID", PROJECT, SCHEMA);
        String expectSQL = "select \"T1\".\"PRICE\", \"T1\".\"ITEM_COUNT\", \"T1\".\"ORDER_ID\", "
                + "\"T2\".\"ORDER_ID\", \"T2\".\"BUYER_ID\", \"T2\".\"TEST_DATE_ENC\" "
                + "from TEST_KYLIN_FACT t1 join TEST_ORDER t2 on t1.ORDER_ID = t2.ORDER_ID";
        assertRoughlyEquals(expectSQL, sql);
    }

    @Test
    public void testTransformColumnStartWithNumberOrKeyword() {
        getTestConfig().setProperty("kylin.query.calcite.extras-props.quoting", "DOUBLE_QUOTE");
        prepareBasic();
        prepareMore();
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        TableDesc tableDesc = tableMetadataManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        ColumnDesc[] columns = tableDesc.getColumns();

        ColumnDesc colStartsWithNumber = new ColumnDesc(columns[0]);
        colStartsWithNumber.setId("13");
        colStartsWithNumber.setDatatype("date");
        colStartsWithNumber.setName("2D");
        ColumnDesc colWithKeyword = new ColumnDesc(columns[0]);
        colWithKeyword.setId("14");
        colWithKeyword.setDatatype("date");
        colWithKeyword.setName("YEAR");

        ArrayList<ColumnDesc> columnDescs = Lists.newArrayList(columns);
        columnDescs.add(colStartsWithNumber);
        columnDescs.add(colWithKeyword);

        tableDesc.setColumns(columnDescs.toArray(new ColumnDesc[0]));
        tableMetadataManager.updateTableDesc(tableDesc);
        HackSelectStarWithColumnACL transformer = new HackSelectStarWithColumnACL();
        QueryContext.current().setAclInfo(new QueryContext.AclInfo("u1", Sets.newHashSet("g1"), false));
        String transformed = transformer.convert("select * from TEST_KYLIN_FACT", PROJECT, SCHEMA);
        String expected = "select \"TEST_KYLIN_FACT\".\"ORDER_ID\", " //
                + "\"TEST_KYLIN_FACT\".\"PRICE\", " //
                + "\"TEST_KYLIN_FACT\".\"ITEM_COUNT\", " //
                + "\"TEST_KYLIN_FACT\".\"2D\", " //
                + "\"TEST_KYLIN_FACT\".\"YEAR\" " //
                + "from TEST_KYLIN_FACT";
        Assert.assertEquals(expected, transformed);
    }

    @Test
    public void testTransformColumnStartWithNumberOrKeyword2() {
        getTestConfig().setProperty("kylin.query.calcite.extras-props.quoting", "BACK_TICK");
        prepareBasic();
        prepareMore();
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        TableDesc tableDesc = tableMetadataManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        ColumnDesc[] columns = tableDesc.getColumns();

        ColumnDesc colStartsWithNumber = new ColumnDesc(columns[0]);
        colStartsWithNumber.setId("13");
        colStartsWithNumber.setDatatype("date");
        colStartsWithNumber.setName("2D");
        ColumnDesc colWithKeyword = new ColumnDesc(columns[0]);
        colWithKeyword.setId("14");
        colWithKeyword.setDatatype("date");
        colWithKeyword.setName("YEAR");

        ArrayList<ColumnDesc> columnDescs = Lists.newArrayList(columns);
        columnDescs.add(colStartsWithNumber);
        columnDescs.add(colWithKeyword);

        tableDesc.setColumns(columnDescs.toArray(new ColumnDesc[0]));
        tableMetadataManager.updateTableDesc(tableDesc);
        HackSelectStarWithColumnACL transformer = new HackSelectStarWithColumnACL();
        QueryContext.current().setAclInfo(new QueryContext.AclInfo("u1", Sets.newHashSet("g1"), false));
        String transformed = transformer.convert("select * from TEST_KYLIN_FACT", PROJECT, SCHEMA);
        String expected = "select `TEST_KYLIN_FACT`.`ORDER_ID`, " //
                + "`TEST_KYLIN_FACT`.`PRICE`, " //
                + "`TEST_KYLIN_FACT`.`ITEM_COUNT`, " //
                + "`TEST_KYLIN_FACT`.`2D`, " //
                + "`TEST_KYLIN_FACT`.`YEAR` " //
                + "from TEST_KYLIN_FACT";
        Assert.assertEquals(expected, transformed);
    }

    @Test
    public void testExplainSyntax() {
        HackSelectStarWithColumnACL transformer = new HackSelectStarWithColumnACL();
        String sql = "explain plan for select * from t";
        assertRoughlyEquals(sql, transformer.convert("explain plan for select * from t", PROJECT, SCHEMA));
    }

    @Test
    public void testGetNewSelectClause() {
        prepareBasic();
        final String sql = "select * from TEST_KYLIN_FACT t1 join TEST_ORDER t2 on t1.ORDER_ID = t2.ORDER_ID ";
        final SqlNode sqlNode = getSqlNode(sql);
        QueryContext.AclInfo aclInfo = new QueryContext.AclInfo("u1", Sets.newHashSet("g1"), false);
        String newSelectClause = HackSelectStarWithColumnACL.getNewSelectClause(sqlNode, PROJECT, SCHEMA, aclInfo);
        String expect = "\"T1\".\"PRICE\", \"T1\".\"ITEM_COUNT\", \"T1\".\"ORDER_ID\", "
                + "\"T2\".\"ORDER_ID\", \"T2\".\"BUYER_ID\", \"T2\".\"TEST_DATE_ENC\"";
        assertRoughlyEquals(expect, newSelectClause);

        AclTCR empty = new AclTCR();
        empty.setTable(new AclTCR.Table());
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), PROJECT);
        manager.updateAclTCR(empty, "u1", true);
        manager.updateAclTCR(empty, "g1", false);

        try {
            HackSelectStarWithColumnACL.getNewSelectClause(sqlNode, PROJECT, SCHEMA, aclInfo);
        } catch (Exception e) {
            Assert.assertEquals(NoAuthorizedColsError.class, e.getClass());
        }
    }

    @Test
    public void testGetColsCanAccess() {

        final String sql = "select * from TEST_KYLIN_FACT t1 join TEST_ORDER t2 on t1.ORDER_ID = t2.ORDER_ID";
        final SqlNode sqlNode = getSqlNode(sql);
        QueryContext.AclInfo aclInfo = new QueryContext.AclInfo("u1", Sets.newHashSet("g1"), false);
        List<String> colsCanAccess = HackSelectStarWithColumnACL.getColsCanAccess(sqlNode, PROJECT, SCHEMA, aclInfo);
        Assert.assertEquals(0, colsCanAccess.size());

        prepareBasic();
        colsCanAccess = HackSelectStarWithColumnACL.getColsCanAccess(sqlNode, PROJECT, SCHEMA, aclInfo);
        Assert.assertEquals(6, colsCanAccess.size());
    }

    private SqlNode getSqlNode(String sql) {
        SqlNode sqlNode;
        try {
            sqlNode = CalciteParser.parse(sql);
        } catch (SqlParseException e) {
            throw new RuntimeException("Failed to parse SQL \'" + sql + "\', please make sure the SQL is valid");
        }
        return sqlNode;
    }

    private void assertRoughlyEquals(String expect, String actual) {
        String[] expectSplit = expect.split("\\s+");
        String[] actualSplit = actual.split("\\s+");
        Arrays.sort(expectSplit);
        Arrays.sort(actualSplit);

        Assert.assertArrayEquals(expectSplit, actualSplit);
    }

    private void prepareMore() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), PROJECT);
        AclTCR g1a1 = new AclTCR();
        AclTCR.Table g1t1 = new AclTCR.Table();
        AclTCR.ColumnRow g1cr1 = new AclTCR.ColumnRow();
        AclTCR.Column g1c1 = new AclTCR.Column();
        g1c1.addAll(Arrays.asList("ORDER_ID", "2D", "YEAR"));
        g1cr1.setColumn(g1c1);
        g1t1.put("DEFAULT.TEST_KYLIN_FACT", g1cr1);
        g1a1.setTable(g1t1);
        manager.updateAclTCR(g1a1, "g1", false);
    }

    private void prepareBasic() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), PROJECT);

        AclTCR u1a1 = new AclTCR();
        AclTCR.Table u1t1 = new AclTCR.Table();
        AclTCR.ColumnRow u1cr1 = new AclTCR.ColumnRow();
        AclTCR.Column u1c1 = new AclTCR.Column();
        u1c1.addAll(Arrays.asList("PRICE", "ITEM_COUNT"));
        u1cr1.setColumn(u1c1);

        AclTCR.ColumnRow u1cr2 = new AclTCR.ColumnRow();
        AclTCR.Column u1c2 = new AclTCR.Column();
        u1c2.addAll(Arrays.asList("ORDER_ID", "BUYER_ID", "TEST_DATE_ENC"));
        u1cr2.setColumn(u1c2);
        u1t1.put("DEFAULT.TEST_KYLIN_FACT", u1cr1);
        u1t1.put("DEFAULT.TEST_ORDER", u1cr2);
        u1a1.setTable(u1t1);
        manager.updateAclTCR(u1a1, "u1", true);

        AclTCR g1a1 = new AclTCR();
        AclTCR.Table g1t1 = new AclTCR.Table();
        AclTCR.ColumnRow g1cr1 = new AclTCR.ColumnRow();
        AclTCR.Column g1c1 = new AclTCR.Column();
        g1c1.addAll(Arrays.asList("ORDER_ID"));
        g1cr1.setColumn(g1c1);
        g1t1.put("DEFAULT.TEST_KYLIN_FACT", g1cr1);
        g1a1.setTable(g1t1);
        manager.updateAclTCR(g1a1, "g1", false);
    }
}

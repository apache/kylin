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

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.QueryContext;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.acl.AclTCR;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@MetadataInfo
class HackSelectStarWithColumnACLWithSelectStarCaseSensitiveTest {
    private static final String PROJECT = "default";
    private static final String SCHEMA = "DEFAULT";
    private static final HackSelectStarWithColumnACL TRANSFORMER = new HackSelectStarWithColumnACL();
    QueryContext current = QueryContext.current();

    @BeforeEach
    void setup() {
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "true");
        prepareBasic();
        current.setAclInfo(new QueryContext.AclInfo("u1", Sets.newHashSet("g1"), false));
    }

    @AfterAll
    static void afterAll() {
        QueryContext.current().close();
    }

    @Test
    void testJoin() {
        // without alias
        {
            String sql = "select * from TEST_KYLIN_FACT join TEST_ORDER "
                    + "on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select * from ( " //
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\" "
                    + "join ( select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" from \"DEFAULT\".\"TEST_ORDER\") as \"TEST_ORDER\" "
                    + "on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID";
            Assertions.assertEquals(expected, converted);
        }
        // with alias
        {
            String sql = "select * from TEST_KYLIN_FACT t1 join TEST_ORDER t2 on t1.ORDER_ID = t2.ORDER_ID";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select * from ( " //
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"T1\" "
                    + "join ( select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" from \"DEFAULT\".\"TEST_ORDER\") as \"T2\" "
                    + "on t1.ORDER_ID = t2.ORDER_ID";
            Assertions.assertEquals(expected, converted);
        }
        // with alias without select star
        {
            String sql = "select t1.ORDER_ID from TEST_KYLIN_FACT t1 join TEST_ORDER t2 on t1.ORDER_ID = t2.ORDER_ID";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select t1.ORDER_ID from ( " //
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"T1\" "
                    + "join ( select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" from \"DEFAULT\".\"TEST_ORDER\") as \"T2\" "
                    + "on t1.ORDER_ID = t2.ORDER_ID";
            Assertions.assertEquals(expected, converted);
        }
        // with tow alias
        {
            String sql = "select * from TEST_KYLIN_FACT t1 join TEST_ORDER t2 on t1.ORDER_ID = t2.ORDER_ID"
                    + " join TEST_ORDER t3 on t1.ORDER_ID = t3.ORDER_ID";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select * from ( " //
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"T1\" "
                    + "join ( select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" from \"DEFAULT\".\"TEST_ORDER\") as \"T2\" "
                    + "on t1.ORDER_ID = t2.ORDER_ID "
                    + "join ( select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" from \"DEFAULT\".\"TEST_ORDER\") as \"T3\" "
                    + "on t1.ORDER_ID = t3.ORDER_ID";
            Assertions.assertEquals(expected, converted);
        }
        // with tow alias without select star
        {
            String sql = "select t1.ORDER_ID from TEST_KYLIN_FACT t1 join TEST_ORDER t2 on t1.ORDER_ID = t2.ORDER_ID"
                    + " join TEST_ORDER t3 on t1.ORDER_ID = t3.ORDER_ID";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select t1.ORDER_ID from ( " //
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"T1\" "
                    + "join ( select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" from \"DEFAULT\".\"TEST_ORDER\") as \"T2\" "
                    + "on t1.ORDER_ID = t2.ORDER_ID "
                    + "join ( select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" from \"DEFAULT\".\"TEST_ORDER\") as \"T3\" "
                    + "on t1.ORDER_ID = t3.ORDER_ID";
            Assertions.assertEquals(expected, converted);
        }
        // nested select star
        {
            String sql = "select * from (select * from TEST_KYLIN_FACT) t1 join TEST_ORDER t2 "
                    + "on t1.ORDER_ID = t2.ORDER_ID";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select * from (" //
                    + "select * from ( " //
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\""
                    + ") t1 join ( select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" from \"DEFAULT\".\"TEST_ORDER\""
                    + ") as \"T2\" on t1.ORDER_ID = t2.ORDER_ID";
            Assertions.assertEquals(expected, converted);
        }
    }

    @Test
    void testWithSubQuery() {
        // simple case
        {
            String sql = "with test_order as (select * from test_order)\n"
                    + "select * from TEST_KYLIN_FACT join TEST_ORDER "
                    + "on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "with test_order as (select * from ( "
                    + "select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" from \"DEFAULT\".\"TEST_ORDER\") as \"TEST_ORDER\")\n"
                    + "select * from ( " //
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\" "
                    + "join \"TEST_ORDER\" on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID";
            Assertions.assertEquals(expected, converted);
        }
        // with-item has alias in with-body
        {
            String sql = "with \"TEMP_DEPT\" as (select fpd.order_id, fpd.buyer_id "
                    + "from test_order as fpd group by fpd.order_id, fpd.buyer_id)\n"
                    + "select fpd.order_id, fpd.buyer_id from temp_dept as fpd";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "with \"TEMP_DEPT\" as (select fpd.order_id, fpd.buyer_id from ( "
                    + "select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", \"TEST_ORDER\".\"Test_Date_Enc\" "
                    + "from \"DEFAULT\".\"TEST_ORDER\") as \"FPD\" group by fpd.order_id, fpd.buyer_id)\n"
                    + "select fpd.order_id, fpd.buyer_id from \"TEMP_DEPT\" as \"FPD\"";
            Assertions.assertEquals(expected, converted);
        }
        // some content of with-body reuse with-items
        {
            String sql = "with test_order as (select * from test_order)\n"
                    + "select * from TEST_KYLIN_FACT join (select * from TEST_ORDER) TEST_ORDER "
                    + "on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "with test_order as (select * from ( "
                    + "select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" from \"DEFAULT\".\"TEST_ORDER\") as \"TEST_ORDER\")\n"
                    + "select * from ( " //
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\" "
                    + "join (select * from \"TEST_ORDER\") TEST_ORDER on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID";
            Assertions.assertEquals(expected, converted);
        }
        // all contexts of with-body do not reuse any with-items
        {
            String sql = "with test_order as (select * from test_order)\n"
                    + "select * from TEST_KYLIN_FACT join (select * from \"DEFAULT\".\"TEST_ORDER\") TEST_ORDER "
                    + "on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "with test_order as (select * from ( "
                    + "select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" from \"DEFAULT\".\"TEST_ORDER\") as \"TEST_ORDER\")\n"
                    + "select * from ( " //
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\" " //
                    + "join (select * from ( " //
                    + "select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" "
                    + "from \"DEFAULT\".\"TEST_ORDER\") as \"TEST_ORDER\") TEST_ORDER "
                    + "on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID";
            Assertions.assertEquals(expected, converted);
        }
        // all contexts of with-body reuse with-items
        {
            String sql = "with test_order as (select * from test_order), "
                    + "test_kylin_fact as (select * from test_kylin_fact)\n"
                    + "select * from TEST_KYLIN_FACT join (select * from TEST_ORDER) TEST_ORDER "
                    + "on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "with test_order as (" //
                    + "select * from ( select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" " //
                    + "from \"DEFAULT\".\"TEST_ORDER\") as \"TEST_ORDER\"), " //
                    + "test_kylin_fact as ("
                    + "select * from ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\")\n"
                    + "select * from \"TEST_KYLIN_FACT\" join (select * from \"TEST_ORDER\") TEST_ORDER "
                    + "on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID";
            Assertions.assertEquals(expected, converted);
        }
    }

    @Test
    void testResolvedSorted() {
        {
            String sql = "select " //
                    + " sum(ORDER_ID)," //
                    + " (select sum(ORDER_ID) from TEST_ORDER ) as o2" //
                    + "  from TEST_KYLIN_FACT";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select " //
                    + " sum(ORDER_ID)," //
                    + " (select sum(ORDER_ID) from " //
                    + "( select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" " //
                    + "from \"DEFAULT\".\"TEST_ORDER\") as \"TEST_ORDER\" ) as o2" //
                    + "  from ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\"";
            Assertions.assertEquals(expected, converted);
        }
        {
            String sql = "select " //
                    + " sum(ORDER_ID)," //
                    + " (select sum(ORDER_ID) from TEST_KYLIN_FACT ) as o2" //
                    + "  from TEST_KYLIN_FACT";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select " //
                    + " sum(ORDER_ID)," //
                    + " (select sum(ORDER_ID) from " //
                    + "( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\" ) as o2" //
                    + "  from ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\"";
            Assertions.assertEquals(expected, converted);
        }
    }

    @Test
    void testWithoutSubQueryFrom() {
        // without from
        {
            String sql = "select -((select sum(ORDER_ID) from test_kylin_fact)"
                    + " -(select sum(ORDER_ID) from test_kylin_fact)" //
                    + ") as a1";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select -((select sum(ORDER_ID) from" //
                    + " ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\")" //
                    + " -(select sum(ORDER_ID) from" //
                    + " ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\")" //
                    + ") as a1";
            Assertions.assertEquals(expected, converted);
        }
        // without from and with SqlIdentifier in selectList
        {
            String sql = "select -((select sum(ORDER_ID) from test_kylin_fact)"
                    + " -(select sum(ORDER_ID) from test_kylin_fact)" //
                    + ") as a1, sum(ORDER_ID) as a2";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select -((select sum(ORDER_ID) from" //
                    + " ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\")" //
                    + " -(select sum(ORDER_ID) from" //
                    + " ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\")" //
                    + ") as a1, sum(ORDER_ID) as a2";
            Assertions.assertEquals(expected, converted);
        }
        // without from and with SqlWith in selectList
        {
            String sql = "select -((" //
                    + "with test_kylin_fact1 as (select * from test_kylin_fact) "
                    + "select sum(ORDER_ID) from test_kylin_fact1)" //
                    + " -(select sum(PRICE) from test_kylin_fact)" //
                    + ") as a1, sum(ORDER_ID) as a2";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select -((" //
                    + "with test_kylin_fact1 as ("
                    + "select * from ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\") " //
                    + "select sum(ORDER_ID) from \"TEST_KYLIN_FACT1\")" //
                    + " -(select sum(PRICE) from" //
                    + " ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\")" //
                    + ") as a1, sum(ORDER_ID) as a2";
            Assertions.assertEquals(expected, converted);
        }
        {
            String sql = "select -((" //
                    + "with test_order as (select * from test_order), "
                    + "test_kylin_fact1 as (select * from test_kylin_fact) "
                    + "select sum(TEST_KYLIN_FACT1.ORDER_ID) from TEST_KYLIN_FACT1"
                    + " join (select * from TEST_ORDER) TEST_ORDER "
                    + "on TEST_KYLIN_FACT1.ORDER_ID = TEST_ORDER.ORDER_ID limit 1)" //
                    + " -(select sum(PRICE) from test_kylin_fact)" //
                    + ") as a1, sum(ORDER_ID) as a2";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select -((" //
                    + "with test_order as (" //
                    + "select * from ( select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" " //
                    + "from \"DEFAULT\".\"TEST_ORDER\") as \"TEST_ORDER\"), " //
                    + "test_kylin_fact1 as ("
                    + "select * from ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\") "
                    + "select sum(TEST_KYLIN_FACT1.ORDER_ID) from \"TEST_KYLIN_FACT1\""
                    + " join (select * from \"TEST_ORDER\") TEST_ORDER "
                    + "on TEST_KYLIN_FACT1.ORDER_ID = TEST_ORDER.ORDER_ID limit 1)" //
                    + " -(select sum(PRICE) from" //
                    + " ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\")" //
                    + ") as a1, sum(ORDER_ID) as a2";
            Assertions.assertEquals(expected, converted);
        }
        // without from and with SqlJoin in selectList
        {
            String sql = "select -((" //
                    + "select sum(t1.ORDER_ID) from (select * from TEST_KYLIN_FACT) t1 join TEST_ORDER t2 "
                    + "on t1.ORDER_ID = t2.ORDER_ID)" //
                    + " -(select sum(PRICE) from test_kylin_fact)" //
                    + ") as a1, sum(ORDER_ID) as a2";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select -((" //
                    + "select sum(t1.ORDER_ID) from (" //
                    + "select * from ( " //
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\""
                    + ") t1 join ( select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" from \"DEFAULT\".\"TEST_ORDER\""
                    + ") as \"T2\" on t1.ORDER_ID = t2.ORDER_ID)"//
                    + " -(select sum(PRICE) from" //
                    + " ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\")" //
                    + ") as a1, sum(ORDER_ID) as a2";
            Assertions.assertEquals(expected, converted);
        }
        // without from with null
        {
            String sql = "select (select sum(ORDER_ID), null from test_kylin_fact)"
                    + ",(select sum(ORDER_ID) from test_kylin_fact) as a1";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select (select sum(ORDER_ID), null from" //
                    + " ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\")" //
                    + ",(select sum(ORDER_ID) from" //
                    + " ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\") as a1";
            Assertions.assertEquals(expected, converted);
        }
    }

    @Test
    void testUnion() {
        // without outer select
        {
            String sql = "select * from test_order union select * from test_order";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select * from ( " //
                    + "select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" from \"DEFAULT\".\"TEST_ORDER\") as \"TEST_ORDER\" "
                    + "union select * from ( " //
                    + "select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" from \"DEFAULT\".\"TEST_ORDER\") as \"TEST_ORDER\"";
            Assertions.assertEquals(expected, converted);
        }
        // with outer select
        {
            String sql = "select * from (select * from test_order union select * from test_order)";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select * from (select * from ( " //
                    + "select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" from \"DEFAULT\".\"TEST_ORDER\") as \"TEST_ORDER\" "
                    + "union select * from ( " //
                    + "select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" from \"DEFAULT\".\"TEST_ORDER\") as \"TEST_ORDER\")";
            Assertions.assertEquals(expected, converted);
        }
    }

    @Test
    void testInSubQuery() {
        String sql = "select * from TEST_KYLIN_FACT "
                + "where ITEM_COUNT in (select ITEM_COUNT from (select * from TEST_KYLIN_FACT) )";
        String expected = "select * from ( "
                + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\" "
                + "where ITEM_COUNT in (select ITEM_COUNT from (select * from TEST_KYLIN_FACT) )";
        String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
        Assertions.assertEquals(expected, converted);
    }

    @Test
    void testCaseWhen() {
        {
            String sql = "select (case when ITEM_COUNT > 0 " //
                    + "then (case when order_id > 0 then order_id else 1 end)  " //
                    + "else null end)\n" //
                    + "from TEST_KYLIN_FACT";
            String expected = "select (case when ITEM_COUNT > 0 " //
                    + "then (case when order_id > 0 then order_id else 1 end)  " //
                    + "else null end)\n" //
                    + "from ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", " //
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" " //
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\"";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            Assertions.assertEquals(expected, converted);
        }

        {
            String sql = "select * from test_kylin_fact " //
                    + "where case when ITEM_COUNT > 10 then item_count else 0 end";
            String expected = "select * from ( " //
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", " //
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" " //
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\" "
                    + "where case when ITEM_COUNT > 10 then item_count else 0 end";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            Assertions.assertEquals(expected, converted);
        }
    }

    @Test
    void testSingleTable() {
        // without limit
        {
            String sql = "select * from \"DEFAULT\".\"TEST_KYLIN_FACT\"";
            String expected = "select * from ( "
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\"";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            Assertions.assertEquals(expected, converted);
        }
        // with alias
        {
            String sql = "select * from test_kylin_fact as test_kylin_fact";
            String expected = "select * from ( "
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\"";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            Assertions.assertEquals(expected, converted);
        }
        // with limit-offset
        {
            String sql = "select * from test_kylin_fact as test_kylin_fact limit 10 offset 2";
            String expected = "select * from ( "
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\" limit 10 offset 2";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            Assertions.assertEquals(expected, converted);
        }
        // agg
        {
            String sql = "select count(*) from \"DEFAULT\".\"TEST_KYLIN_FACT\"";
            String expected = "select count(*) from ( "
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\"";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            Assertions.assertEquals(expected, converted);
        }
    }

    @Test
    void testKeywordAsColName() {
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

        List<ColumnDesc> columnDescs = Lists.newArrayList(columns);
        columnDescs.add(colStartsWithNumber);
        columnDescs.add(colWithKeyword);

        tableDesc.setColumns(columnDescs.toArray(new ColumnDesc[0]));
        tableMetadataManager.updateTableDesc(tableDesc);

        String sql = "select * from TEST_KYLIN_FACT";
        String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
        String expected = "select * from ( " //
                + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                + "\"TEST_KYLIN_FACT\".\"item_COUNT\", \"TEST_KYLIN_FACT\".\"2D\", \"TEST_KYLIN_FACT\".\"YEAR\" "
                + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\"";
        Assertions.assertEquals(expected, converted);
    }

    @Test
    void testColumnNameStartsWithNumber() {
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

        List<ColumnDesc> columnDescs = Lists.newArrayList(columns);
        columnDescs.add(colStartsWithNumber);
        columnDescs.add(colWithKeyword);

        tableDesc.setColumns(columnDescs.toArray(new ColumnDesc[0]));
        tableMetadataManager.updateTableDesc(tableDesc);

        String sql = "select * from TEST_KYLIN_FACT";
        String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
        String expected = "select * from ( " //
                + "select \"TEST_KYLIN_FACT\".\"order_id\", " //
                + "\"TEST_KYLIN_FACT\".\"PRICE\", " //
                + "\"TEST_KYLIN_FACT\".\"item_COUNT\", " //
                + "\"TEST_KYLIN_FACT\".\"2D\", " //
                + "\"TEST_KYLIN_FACT\".\"YEAR\" " //
                + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\"";
        Assertions.assertEquals(expected, converted);
    }

    @Test
    void testExplainSyntax() {
        String sql = "explain plan for select * from t";
        Assertions.assertEquals(sql, TRANSFORMER.convert("explain plan for select * from t", PROJECT, SCHEMA));
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
        g1c1.add("ORDER_ID");
        g1cr1.setColumn(g1c1);
        g1t1.put("DEFAULT.TEST_KYLIN_FACT", g1cr1);
        g1a1.setTable(g1t1);
        manager.updateAclTCR(g1a1, "g1", false);
    }

    @Test
    void testJoinCaseSensitiveEnable() {
        {
            String sql = "select * from \"DEFAULT\".\"TEST_KYLIN_FACT\"";
            String expected = "select * from ( "
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\"";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            Assertions.assertEquals(expected, converted);
        }
        {
            String sql = "select count(*) from \"DEFAULT\".\"TEST_KYLIN_FACT\"";
            String expected = "select count(*) from ( "
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\"";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            Assertions.assertEquals(expected, converted);
        }
        {
            String sql = "select (case when ITEM_COUNT > 0 " //
                    + "then (case when order_id > 0 then order_id else 1 end)  " //
                    + "else null end)\n" //
                    + "from TEST_KYLIN_FACT";
            String expected = "select (case when ITEM_COUNT > 0 " //
                    + "then (case when order_id > 0 then order_id else 1 end)  " //
                    + "else null end)\n" //
                    + "from ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", " //
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" " //
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\"";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            Assertions.assertEquals(expected, converted);
        }
        {
            String sql = "select * from TEST_KYLIN_FACT "
                    + "where ITEM_COUNT in (select ITEM_COUNT from (select * from TEST_KYLIN_FACT) )";
            String expected = "select * from ( "
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\" "
                    + "where ITEM_COUNT in (select ITEM_COUNT from (select * from TEST_KYLIN_FACT) )";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            Assertions.assertEquals(expected, converted);
        }
        {
            String sql = "select -((select sum(ORDER_ID) from test_kylin_fact)"
                    + " -(select sum(ORDER_ID) from test_kylin_fact)" //
                    + ") as a1";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select -((select sum(ORDER_ID) from" //
                    + " ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\")" //
                    + " -(select sum(ORDER_ID) from" //
                    + " ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\")" //
                    + ") as a1";
            Assertions.assertEquals(expected, converted);
        }
        {
            String sql = "select " //
                    + " sum(ORDER_ID)," //
                    + " (select sum(ORDER_ID) from TEST_KYLIN_FACT ) as o2" //
                    + "  from TEST_KYLIN_FACT";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select " //
                    + " sum(ORDER_ID)," //
                    + " (select sum(ORDER_ID) from " //
                    + "( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\" ) as o2" //
                    + "  from ( select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\"";
            Assertions.assertEquals(expected, converted);
        }
        {
            String sql = "select * from TEST_KYLIN_FACT join TEST_ORDER "
                    + "on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select * from ( " //
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\" "
                    + "join ( select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" from \"DEFAULT\".\"TEST_ORDER\") as \"TEST_ORDER\" "
                    + "on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID";
            Assertions.assertEquals(expected, converted);
        }
        {
            String sql = "with test_order as (select * from test_order)\n"
                    + "select * from TEST_KYLIN_FACT join TEST_ORDER "
                    + "on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID";
            String converted = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "with test_order as (select * from ( "
                    + "select \"TEST_ORDER\".\"order_id\", \"TEST_ORDER\".\"BUYER_ID\", "
                    + "\"TEST_ORDER\".\"Test_Date_Enc\" from \"DEFAULT\".\"TEST_ORDER\") as \"TEST_ORDER\")\n"
                    + "select * from ( " //
                    + "select \"TEST_KYLIN_FACT\".\"order_id\", \"TEST_KYLIN_FACT\".\"PRICE\", "
                    + "\"TEST_KYLIN_FACT\".\"item_COUNT\" "
                    + "from \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\" "
                    + "join \"TEST_ORDER\" on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID";
            Assertions.assertEquals(expected, converted);
        }
    }
}

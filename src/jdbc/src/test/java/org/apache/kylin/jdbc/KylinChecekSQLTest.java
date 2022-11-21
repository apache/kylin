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

package org.apache.kylin.jdbc;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class KylinChecekSQLTest {

    @Test
    public void testQueryPlaceholder() {
        String sql1 = "select SELLER_ID from KYLIN_SALES  where SELLER_ID = 10000455";
        String sql2 = "select SELLER_ID from KYLIN_SALES  where SELLER_ID = ?";
        String sql3 = "select SELLER_ID from KYLIN_SALES  where SELLER_ID = ? or SELLER_ID = ? ";
        String sql4 = "select SELLER_ID from KYLIN_SALES  where SELLER_ID = ? and name = '?name' ";
        String sql5 = "select SELLER_ID from KYLIN_SALES  where SELLER_ID = ? and name = `?name` ";
        String sql6 = "select SELLER_ID from KYLIN_SALES  where SELLER_ID = ? and name = \"?name\" ";
        assertEquals(0, KylinCheckSql.countDynamicPlaceholder(sql1));
        assertEquals(1, KylinCheckSql.countDynamicPlaceholder(sql2));
        assertEquals(2, KylinCheckSql.countDynamicPlaceholder(sql3));
        assertEquals(1, KylinCheckSql.countDynamicPlaceholder(sql4));
        assertEquals(1, KylinCheckSql.countDynamicPlaceholder(sql5));
        assertEquals(1, KylinCheckSql.countDynamicPlaceholder(sql6));
    }
}

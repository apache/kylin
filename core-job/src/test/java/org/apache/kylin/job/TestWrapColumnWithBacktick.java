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

package org.apache.kylin.job;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestWrapColumnWithBacktick {
    @Test
    public void testWrapColumnNameInFlatTable() {
        String input1 = "SELLER_ACCOUNT.ACCOUNT_SELLER_LEVEL";
        String expected1 = "SELLER_ACCOUNT.`ACCOUNT_SELLER_LEVEL`";
        String input2 = "year(TEST_KYLIN_FACT.CAL_DT)";
        String expected2 = "year(TEST_KYLIN_FACT.`CAL_DT`)";
        String input3 = "SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,1)";
        String expected3 = "SUBSTR(SELLER_ACCOUNT.`ACCOUNT_COUNTRY`,0,1)";
        String input4 = "CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)";
        String expected4 = "CONCAT(SELLER_ACCOUNT.`ACCOUNT_ID`, SELLER_COUNTRY.`NAME`)";
        String input5 = "SELLER_ACCOUNT.ACCOUNT_SELLER_LEVEL*1.1";
        String expected5 = "SELLER_ACCOUNT.`ACCOUNT_SELLER_LEVEL`*1.1";
        String input6 = "SELLER_ACCOUNT.ACCOUNT_SELLER_LEVEL*SELLER_ACCOUNT.ACCOUNT_SELLER_LEVEL";
        String expected6 = "SELLER_ACCOUNT.`ACCOUNT_SELLER_LEVEL`*SELLER_ACCOUNT.`ACCOUNT_SELLER_LEVEL`";
        String input7 = "CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, 'HELLO.WORLD')";
        String expected7 =  "CONCAT(SELLER_ACCOUNT.`ACCOUNT_ID`, 'HELLO.WORLD')";
        String input8 = "CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_ACCOUNT.ACCOUNT_ID2)";
        String expected8 =  "CONCAT(SELLER_ACCOUNT.`ACCOUNT_ID`, SELLER_ACCOUNT.`ACCOUNT_ID2`)";
        String input9 = "CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_ACCOUNT.ACCOUNT_ID2, SELLER_ACCOUNT.ACCOUNT_ID3)";
        String expected9 =  "CONCAT(SELLER_ACCOUNT.`ACCOUNT_ID`, SELLER_ACCOUNT.`ACCOUNT_ID2`, SELLER_ACCOUNT.`ACCOUNT_ID3`)";

        assertEquals(expected1, JoinedFlatTable.quoteOriginalColumnWithBacktick(input1));
        assertEquals(expected2, JoinedFlatTable.quoteOriginalColumnWithBacktick(input2));
        assertEquals(expected3, JoinedFlatTable.quoteOriginalColumnWithBacktick(input3));
        assertEquals(expected4, JoinedFlatTable.quoteOriginalColumnWithBacktick(input4));
        assertEquals(expected5, JoinedFlatTable.quoteOriginalColumnWithBacktick(input5));
        assertEquals(expected6, JoinedFlatTable.quoteOriginalColumnWithBacktick(input6));
        assertEquals(expected7, JoinedFlatTable.quoteOriginalColumnWithBacktick(input7));
        assertEquals(expected8, JoinedFlatTable.quoteOriginalColumnWithBacktick(input8));
        assertEquals(expected9, JoinedFlatTable.quoteOriginalColumnWithBacktick(input9));
    }
}

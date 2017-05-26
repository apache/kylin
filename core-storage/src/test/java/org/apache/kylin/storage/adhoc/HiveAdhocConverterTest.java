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

package org.apache.kylin.storage.adhoc;

import junit.framework.TestCase;
import org.junit.Test;


public class HiveAdhocConverterTest extends TestCase {
    @Test
    public void testSringReplace() {
        String originString = "select count(*) as cnt from test_kylin_fact where char_length(lstg_format_name) < 10";
        String replacedString = HiveAdhocConverter
            .replaceString(originString, "char_length", "length");
        assertEquals(replacedString, "select count(*) as cnt from test_kylin_fact where length(lstg_format_name) < 10");
    }

    @Test
    public void testExtractReplace() {
        String originString = "ignore EXTRACT(YEAR FROM KYLIN_CAL_DT.CAL_DT) ignore";
        String replacedString = HiveAdhocConverter.extractReplace(originString);
        assertEquals(replacedString, "ignore YEAR(KYLIN_CAL_DT.CAL_DT) ignore");
    }

    @Test
    public void testCastReplace() {
        String originString = "ignore EXTRACT(YEAR FROM CAST(KYLIN_CAL_DT.CAL_DT AS INTEGER)) ignore";
        String replacedString = HiveAdhocConverter.castRepalce(originString);
        assertEquals(replacedString, "ignore EXTRACT(YEAR FROM CAST(KYLIN_CAL_DT.CAL_DT AS int)) ignore");
    }

    @Test
    public void testSubqueryReplace() {
        String originString = "select seller_id,lstg_format_name,sum(price) from (select * from test_kylin_fact where (lstg_format_name='FP-GTC') limit 20) group by seller_id,lstg_format_name";
        String replacedString = HiveAdhocConverter.subqueryRepalce(originString);
        assertEquals(replacedString, "select seller_id,lstg_format_name,sum(price) from (select * from test_kylin_fact where (lstg_format_name='FP-GTC') limit 20) as alias group by seller_id,lstg_format_name");
    }

    @Test
    public void testConcatReplace() {
        String originString = "select count(*) as cnt from test_kylin_fact where lstg_format_name||'a'='ABINa'";
        String replacedString = HiveAdhocConverter.concatReplace(originString);
        assertEquals(replacedString, "select count(*) as cnt from test_kylin_fact where concat(lstg_format_name,'a')='ABINa'");
    }

}

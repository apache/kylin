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

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReplaceStringWithVarcharTest extends NLocalFileMetadataTestCase {
    private static final ReplaceStringWithVarchar replaceStringWithVarchar = new ReplaceStringWithVarchar();

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBase() {
        // test cases ref: https://dev.mysql.com/doc/refman/8.0/en/select.html
        String[] actuals = new String[] {
                // select, from, where, like, group by, having, order by, subclause
                "Select 0 as STRING, cast(D1.c1 as STRING) as c2 from (select distinct substring(cast(T33458.CAL_DT as STRING), 1, 30) as c1 from TEST_KYLIN_FACT T33458) D1 where cast(D1.c1 as STRING) like cast('2012-01%' as STRING) group by cast(D1.c1 as STRING) having cast(D1.c1 as STRING)>'2012-01-01' order by cast(D1.c1 as STRING)",
                // over(window)
                "select TRANS_ID, CAL_DT, LSTG_FORMAT_NAME, MAX(CAL_DT) over (PARTITION BY CAST(LSTG_FORMAT_NAME AS STRING)) from TEST_KYLIN_FACT",
                // case when else
                "select case cast(CAL_DT as STRING) when cast('2012-01-13' as STRING) THEN cast('1' as STRING) ELSE cast('null' as STRING) END AS STRING, CAL_DT from TEST_KYLIN_FACT order by cast(CAL_DT as STRING)",
                // join
                "select * from TEST_KYLIN_FACT a left join EDW.TEST_CAL_DT b on cast(a.CAL_DT as STRING)=cast(b.CAL_DT as STRING) limit 100",
                // union
                "select * from TEST_KYLIN_FACT where cast(LSTG_FORMAT_NAME as STRING)='FP-GTC' union select * from TEST_KYLIN_FACT where cast(LSTG_FORMAT_NAME as STRING)='Auction' limit 100",
                // with
                "WITH t1 AS (Select 0 as STRING, cast(D1.c1 as STRING) as c2 from (select distinct substring(cast(T33458.CAL_DT as STRING), 1, 30) as c1 from TEST_KYLIN_FACT T33458) D1 where cast(D1.c1 as STRING) like cast('2012-01%' as STRING) group by cast(D1.c1 as STRING) having cast(D1.c1 as STRING)>'2012-01-01' order by cast(D1.c1 as STRING)) \n"
                        + "SELECT * from t1\n" + "UNION ALL\n" + "SELECT * from t1" };
        String[] expecteds = new String[] {
                "Select 0 as STRING, cast(D1.c1 as VARCHAR) as c2 from (select distinct substring(cast(T33458.CAL_DT as VARCHAR), 1, 30) as c1 from TEST_KYLIN_FACT T33458) D1 where cast(D1.c1 as VARCHAR) like cast('2012-01%' as VARCHAR) group by cast(D1.c1 as VARCHAR) having cast(D1.c1 as VARCHAR)>'2012-01-01' order by cast(D1.c1 as VARCHAR)",
                "select TRANS_ID, CAL_DT, LSTG_FORMAT_NAME, MAX(CAL_DT) over (PARTITION BY CAST(LSTG_FORMAT_NAME AS VARCHAR)) from TEST_KYLIN_FACT",
                "select case cast(CAL_DT as VARCHAR) when cast('2012-01-13' as VARCHAR) THEN cast('1' as VARCHAR) ELSE cast('null' as VARCHAR) END AS STRING, CAL_DT from TEST_KYLIN_FACT order by cast(CAL_DT as VARCHAR)",
                "select * from TEST_KYLIN_FACT a left join EDW.TEST_CAL_DT b on cast(a.CAL_DT as VARCHAR)=cast(b.CAL_DT as VARCHAR) limit 100",
                "select * from TEST_KYLIN_FACT where cast(LSTG_FORMAT_NAME as VARCHAR)='FP-GTC' union select * from TEST_KYLIN_FACT where cast(LSTG_FORMAT_NAME as VARCHAR)='Auction' limit 100",
                "WITH t1 AS (Select 0 as STRING, cast(D1.c1 as VARCHAR) as c2 from (select distinct substring(cast(T33458.CAL_DT as VARCHAR), 1, 30) as c1 from TEST_KYLIN_FACT T33458) D1 where cast(D1.c1 as VARCHAR) like cast('2012-01%' as VARCHAR) group by cast(D1.c1 as VARCHAR) having cast(D1.c1 as VARCHAR)>'2012-01-01' order by cast(D1.c1 as VARCHAR)) \n"
                        + "SELECT * from t1\n" + "UNION ALL\n" + "SELECT * from t1" };
        for (int i = 0; i < actuals.length; ++i) {
            String transformedActual = replaceStringWithVarchar.transform(actuals[i], "", "");
            Assert.assertEquals(expecteds[i], transformedActual);
            System.out.println("\nTRANSFORM SUCCEED\nBEFORE:\n" + actuals[i] + "\nAFTER:\n" + expecteds[i]);
        }
    }
}

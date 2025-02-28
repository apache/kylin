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

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@MetadataInfo
class DefaultQueryTransformerTest {

    @BeforeEach
    public void setUp() throws Exception {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.optimized-sum-cast-double-rule-enabled", "false");
    }

    @Test
    public void transformSumNumericLiteral() {
        DefaultQueryTransformer transformer = new DefaultQueryTransformer();
        String originSql = "select sum  (1) from kylin_sales";
        String transformedSql = transformer.transform(originSql, "", "");
        assertTrue("select COUNT(1) from kylin_sales".equalsIgnoreCase(transformedSql));

        originSql = "select sum(-1.0  ) from kylin_sales";
        transformedSql = transformer.transform(originSql, "", "");
        assertTrue("select -1.0 * COUNT(1) from kylin_sales".equalsIgnoreCase(transformedSql));

        originSql = "select sum( 3.14159   ) from kylin_sales";
        transformedSql = transformer.transform(originSql, "", "");
        assertTrue("select 3.14159 * COUNT(1) from kylin_sales".equalsIgnoreCase(transformedSql));
    }

    @Test
    void sumOfCastTransform() {
        DefaultQueryTransformer transformer = new DefaultQueryTransformer();

        String fnConvertSumSql = "select SUM(CAST(LSTG_SITE_ID AS DOUBLE)) from KYLIN_SALES group by LSTG_SITE_ID";
        String correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(LSTG_SITE_ID) from KYLIN_SALES group by LSTG_SITE_ID".equalsIgnoreCase(correctSql));

        //test SQL contains blank
        //Case one blank interval
        fnConvertSumSql = "select SUM ( CAST ( LSTG_SITE_ID AS DOUBLE ) ) from KYLIN_SALES group by LSTG_SITE_ID";
        correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(LSTG_SITE_ID) from KYLIN_SALES group by LSTG_SITE_ID".equalsIgnoreCase(correctSql));

        //Case multi blank interval
        fnConvertSumSql = "select  SUM (  CAST  (  LSTG_SITE_ID  AS  DOUBLE ) ) from KYLIN_SALES group by LSTG_SITE_ID";
        correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(LSTG_SITE_ID) from KYLIN_SALES group by LSTG_SITE_ID".equalsIgnoreCase(correctSql));

        //Case one or multi blank interval
        fnConvertSumSql = "select SUM (  CAST(LSTG_SITE_ID   AS      DOUBLE )  ) from KYLIN_SALES group by LSTG_SITE_ID";
        correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(LSTG_SITE_ID) from KYLIN_SALES group by LSTG_SITE_ID".equalsIgnoreCase(correctSql));

        //test SQL contains multi sum
        fnConvertSumSql = "select SUM(CAST(LSTG_SITE_ID AS DOUBLE)), SUM(CAST(price AS DOUBLE)) from KYLIN_SALES group by LSTG_SITE_ID";
        correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(LSTG_SITE_ID), sum(price) from KYLIN_SALES group by LSTG_SITE_ID"
                .equalsIgnoreCase(correctSql));
    }

    @Test
    void functionEscapeTransform() {
        DefaultQueryTransformer transformer = new DefaultQueryTransformer();

        String fnConvertSumSql = "select {fn EXTRACT(YEAR from PART_DT)} from KYLIN_SALES";
        String correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select EXTRACT(YEAR from PART_DT) from KYLIN_SALES".equalsIgnoreCase(correctSql));
    }

    @Test
    void testTransformIntervalFunction() {
        DefaultQueryTransformer transformer = new DefaultQueryTransformer();
        String sql = "select ( date '2001-09-28' + interval floor(1.5) day ) from test_kylin_fact";
        String correctSql = transformer.transform(sql, "", "");
        assertTrue("select ( date '2001-09-28' + interval '1' day ) from test_kylin_fact".equalsIgnoreCase(correctSql));

        sql = "select ( date '2001-09-28' + interval floor(3.) day ) from test_kylin_fact";
        correctSql = transformer.transform(sql, "", "");
        assertTrue("select ( date '2001-09-28' + interval '3' day ) from test_kylin_fact".equalsIgnoreCase(correctSql));
    }
}

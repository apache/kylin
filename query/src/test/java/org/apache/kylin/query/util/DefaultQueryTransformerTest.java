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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class DefaultQueryTransformerTest {

    @Test
    public void SumOfFnConvertTransform() throws Exception {
        DefaultQueryTransformer transformer = new DefaultQueryTransformer();

        String fnConvertSumSql = "select sum({fn convert(\"LSTG_SITE_ID\", SQL_DOUBLE)}) from KYLIN_SALES group by LSTG_SITE_ID";
        String correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(\"LSTG_SITE_ID\") from KYLIN_SALES group by LSTG_SITE_ID".equalsIgnoreCase(correctSql));

        //test SQL contains blank
        //Case one blank interval
        fnConvertSumSql = "select sum ( { fn convert( \"LSTG_SITE_ID\" , SQL_DOUBLE) } ) from KYLIN_SALES group by LSTG_SITE_ID";
        correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(\"LSTG_SITE_ID\") from KYLIN_SALES group by LSTG_SITE_ID".equalsIgnoreCase(correctSql));

        //Case multi blank interval
        fnConvertSumSql = "select SUM  (  {  fn  convert(  \"LSTG_SITE_ID\"  ,  SQL_DOUBLE  )  }  ) from KYLIN_SALES group by LSTG_SITE_ID";
        correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(\"LSTG_SITE_ID\") from KYLIN_SALES group by LSTG_SITE_ID".equalsIgnoreCase(correctSql));

        //Case one or multi blank interval
        fnConvertSumSql = "select SUM(  { fn convert( \"LSTG_SITE_ID\"  , SQL_DOUBLE  ) }  ) from KYLIN_SALES group by LSTG_SITE_ID";
        correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(\"LSTG_SITE_ID\") from KYLIN_SALES group by LSTG_SITE_ID".equalsIgnoreCase(correctSql));

        //test exception case of "... fnconvert ..."
        fnConvertSumSql = "select SUM ({fnconvert(\"LSTG_SITE_ID\", SQL_DOUBLE)}) from KYLIN_SALES group by LSTG_SITE_ID";
        correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertFalse("select sum(\"LSTG_SITE_ID\") from KYLIN_SALES group by LSTG_SITE_ID".equalsIgnoreCase(correctSql));

        //test SQL contains multi sum
        fnConvertSumSql = "select SUM({fn convert(\"LSTG_SITE_ID\", SQL_DOUBLE)}), SUM({fn convert(\"price\", SQL_DOUBLE)}) from KYLIN_SALES group by LSTG_SITE_ID";
        correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(\"LSTG_SITE_ID\"), sum(\"price\") from KYLIN_SALES group by LSTG_SITE_ID"
                .equalsIgnoreCase(correctSql));

    }

    @Test
    public void SumOfCastTransform() throws Exception {
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

}
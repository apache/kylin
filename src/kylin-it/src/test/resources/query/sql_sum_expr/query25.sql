--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

-- ISSUE https://olapio.atlassian.net/browse/KE-9299
-- Case 1

SELECT 
    COUNT(*) AS COL1,
    SUM(CASE WHEN "TEST_CAL_DT"."CAL_DT" <= CAST('2012-12-31' AS DATE) THEN PRICE*4 
        WHEN "TEST_CAL_DT"."CAL_DT" > CAST('2012-12-31' AS DATE) THEN 1 ELSE 0 END) AS COL2,
    CASE WHEN SELLER_ID = 0 THEN 0 WHEN SELLER_ID <= 1 THEN 1 ELSE SELLER_ID END AS COL3,
    "TEST_CAL_DT"."CAL_DT" AS COL4

FROM "DEFAULT".TEST_KYLIN_FACT AS TEST_KYLIN_FACT

INNER JOIN "DEFAULT".TEST_ORDER AS TEST_ORDER ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID
INNER JOIN EDW.TEST_CAL_DT AS TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT
INNER JOIN "DEFAULT".TEST_CATEGORY_GROUPINGS AS TEST_CATEGORY_GROUPINGS ON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID
INNER JOIN EDW.TEST_SITES AS TEST_SITES ON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID
INNER JOIN EDW.TEST_SELLER_TYPE_DIM AS TEST_SELLER_TYPE_DIM ON TEST_KYLIN_FACT.SLR_SEGMENT_CD = TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD
INNER JOIN "DEFAULT".TEST_ACCOUNT AS SELLER_ACCOUNT ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID
INNER JOIN "DEFAULT".TEST_ACCOUNT AS BUYER_ACCOUNT ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID
INNER JOIN "DEFAULT".TEST_COUNTRY AS SELLER_COUNTRY ON SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY
INNER JOIN "DEFAULT".TEST_COUNTRY AS BUYER_COUNTRY ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY

WHERE LSTG_FORMAT_NAME='FP-GTC'
GROUP BY 
CASE WHEN SELLER_ID = 0 THEN 0 WHEN SELLER_ID <= 1 THEN 1 ELSE SELLER_ID END,
"TEST_CAL_DT"."CAL_DT"
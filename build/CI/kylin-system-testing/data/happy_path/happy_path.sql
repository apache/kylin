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

SELECT
KYLIN_SALES.PART_DT,
KYLIN_CAL_DT.YEAR_BEG_DT,
SUM(BUYER_ACCOUNT.ACCOUNT_BUYER_LEVEL) as BUYER_LEVEL_SUM,
COUNT(DISTINCT KYLIN_SALES.SELLER_ID) as SELLER_CNT_HLL,
SUM(KYLIN_SALES.PRICE) as GMV,
PERCENTILE(KYLIN_SALES.PRICE, 0.5) as PRICE_PERCENTILE,
COUNT(DISTINCT KYLIN_SALES.ITEM_ID) as ITEM_COUNT
FROM KYLIN_SALES
INNER JOIN KYLIN_CAL_DT as KYLIN_CAL_DT
ON KYLIN_SALES.PART_DT = KYLIN_CAL_DT.CAL_DT
INNER JOIN KYLIN_CATEGORY_GROUPINGS
ON KYLIN_SALES.LEAF_CATEG_ID = KYLIN_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND KYLIN_SALES.LSTG_SITE_ID = KYLIN_CATEGORY_GROUPINGS.SITE_ID
INNER JOIN KYLIN_ACCOUNT as BUYER_ACCOUNT
ON KYLIN_SALES.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID
INNER JOIN KYLIN_ACCOUNT as SELLER_ACCOUNT
ON KYLIN_SALES.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID
INNER JOIN KYLIN_COUNTRY as BUYER_COUNTRY
ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY
INNER JOIN KYLIN_COUNTRY as SELLER_COUNTRY
ON SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY
WHERE PART_DT >= '2012-12-25' and PART_DT < '2013-01-05' and SELLER_COUNTRY.COUNTRY in ('CN')
GROUP BY KYLIN_SALES.PART_DT, KYLIN_CAL_DT.YEAR_BEG_DT
ORDER BY KYLIN_SALES.PART_DT, KYLIN_CAL_DT.YEAR_BEG_DT;


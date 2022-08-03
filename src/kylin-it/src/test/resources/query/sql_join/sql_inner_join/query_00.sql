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
select sum(ITEM_COUNT) as ITEM_CNT
FROM TEST_KYLIN_FACT, TEST_ORDER, TEST_ACCOUNT as BUYER_ACCOUNT,
TEST_ACCOUNT as SELLER_ACCOUNT, EDW.TEST_CAL_DT as TEST_CAL_DT, TEST_CATEGORY_GROUPINGS,
EDW.TEST_SITES as TEST_SITES, EDW.TEST_SELLER_TYPE_DIM as TEST_SELLER_TYPE_DIM,
TEST_COUNTRY as BUYER_COUNTRY, TEST_COUNTRY as SELLER_COUNTRY
where TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID
AND TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID
AND TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID
AND TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT
AND TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID
AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID
AND TEST_KYLIN_FACT.SLR_SEGMENT_CD = TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD
AND BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY
AND SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY
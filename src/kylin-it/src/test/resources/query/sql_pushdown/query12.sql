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
    "NAME"
,   COUNT("TRANS_ID")   AS  "C1"
FROM
    "DEFAULT"."TEST_KYLIN_FACT"
INNER JOIN
    "DEFAULT"."TEST_ACCOUNT" ON  "TEST_KYLIN_FACT"."SELLER_ID"   =   "TEST_ACCOUNT"."ACCOUNT_ID"
INNER JOIN
    EDW.TEST_CAL_DT AS  TEST_CAL_DT ON TEST_ACCOUNT.CAL_DT =   TEST_CAL_DT.CAL_DT
INNER JOIN
    TEST_CATEGORY_GROUPINGS ON TEST_ACCOUNT.LEAF_CATEG_ID  =  TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND TEST_ACCOUNT.LSTG_SITE_ID   =   TEST_CATEGORY_GROUPINGS.SITE_ID
INNER JOIN
    EDW.TEST_SITES  AS  TEST_SITES ON TEST_ACCOUNT.LSTG_SITE_ID   =   TEST_SITES.SITE_ID
INNER JOIN
    "DEFAULT"."TEST_COUNTRY" ON  "TEST_ACCOUNT"."ACCOUNT_COUNTRY"    =   "TEST_COUNTRY"."COUNTRY"
GROUP BY
    "NAME"

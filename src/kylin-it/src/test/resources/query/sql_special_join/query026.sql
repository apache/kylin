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

-- ISSUE #3106

SELECT
  "NAME",
  count(*) AS "C1"
FROM (SELECT
  "OTBL"."TRANS_ID",
  "ITBL"."NAME"
FROM (SELECT
  "OTBL"."TRANS_ID",
  "OTBL"."CAL_DT",
  "OTBL"."LSTG_FORMAT_NAME",
  "OTBL"."LEAF_CATEG_ID",
  "OTBL"."LSTG_SITE_ID",
  "OTBL"."PRICE",
  "OTBL"."SELLER_ID",
  "ITBL"."ACCOUNT_ID",
  "ITBL"."ACCOUNT_BUYER_LEVEL",
  "ITBL"."ACCOUNT_SELLER_LEVEL",
  "ITBL"."ACCOUNT_COUNTRY"
FROM "DEFAULT"."TEST_KYLIN_FACT" AS "OTBL"
INNER JOIN "DEFAULT"."TEST_ACCOUNT" AS "ITBL"
  ON ("OTBL"."SELLER_ID" = "ITBL"."ACCOUNT_ID")
  INNER JOIN edw.test_cal_dt as test_cal_dt
ON OTBL.cal_dt = test_cal_dt.cal_dt
inner JOIN test_category_groupings
 ON OTBL.leaf_categ_id = test_category_groupings.leaf_categ_id AND OTBL.lstg_site_id = test_category_groupings.site_id
 inner JOIN edw.test_sites as test_sites
 ON OTBL.lstg_site_id = test_sites.site_id) AS "OTBL"
INNER JOIN "DEFAULT"."TEST_COUNTRY" AS "ITBL"
  ON ("OTBL"."ACCOUNT_COUNTRY" = "ITBL"."COUNTRY")) AS "ITBL"

GROUP BY "NAME"

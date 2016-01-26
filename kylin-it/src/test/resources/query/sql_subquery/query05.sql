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

SELECT  SUM("TEST_KYLIN_FACT"."PRICE") AS "sum_PRICE_ok" 
	FROM "TEST_KYLIN_FACT"
    INNER JOIN "EDW"."TEST_CAL_DT" AS "TEST_CAL_DT" ON ("TEST_KYLIN_FACT"."CAL_DT" = "TEST_CAL_DT"."CAL_DT")
     inner JOIN test_category_groupings
 ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id
 inner JOIN edw.test_sites as test_sites
 ON test_kylin_fact.lstg_site_id = test_sites.site_id

    INNER JOIN (
     SELECT COUNT(1) AS "XTableau_join_flag",
      SUM("TEST_KYLIN_FACT"."PRICE") AS "X__alias__A",
       "TEST_KYLIN_FACT"."CAL_DT" AS "none_CAL_DT_ok"   FROM "TEST_KYLIN_FACT"
         INNER JOIN "EDW"."TEST_CAL_DT" AS "TEST_CAL_DT" ON ("TEST_KYLIN_FACT"."CAL_DT" = "TEST_CAL_DT"."CAL_DT")
          inner JOIN test_category_groupings
 ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id
 inner JOIN edw.test_sites as test_sites
 ON test_kylin_fact.lstg_site_id = test_sites.site_id
     GROUP BY "TEST_KYLIN_FACT"."CAL_DT"   ORDER BY 2 DESC   LIMIT 10  ) "t0" ON ("TEST_KYLIN_FACT"."CAL_DT" = "t0"."none_CAL_DT_ok") 
    GROUP BY "TEST_KYLIN_FACT"."CAL_DT"

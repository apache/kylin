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
-- non-equi join with equi joins
SELECT TEST_CATEGORY_GROUPINGS.LEAF_CATEG_NAME, count(TEST_ACC.COUNTRY)
FROM
TEST_CATEGORY_GROUPINGS
LEFT JOIN
(
  select TEST_COUNTRY.COUNTRY as COUNTRY, ACCOUNT_BUYER_LEVEL, ACCOUNT_SELLER_LEVEL
  FROM TEST_COUNTRY
  LEFT JOIN TEST_ACCOUNT
  ON TEST_ACCOUNT.ACCOUNT_COUNTRY = TEST_COUNTRY.COUNTRY
) TEST_ACC
ON
TEST_CATEGORY_GROUPINGS.LEAF_CATEG_NAME <> TEST_ACC.COUNTRY
AND TEST_CATEGORY_GROUPINGS.SITE_ID > 2
GROUP BY TEST_CATEGORY_GROUPINGS.LEAF_CATEG_NAME
ORDER BY 1,2
LIMIT 10000

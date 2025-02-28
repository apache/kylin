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
-- equi join with non-equi joins
SELECT TEST_ACC.LEAF_CATEG_NAME, count(TEST_ACC.ACCOUNT_COUNTRY), sum(TEST_KYLIN_FACT.ITEM_COUNT)
FROM TEST_KYLIN_FACT
LEFT JOIN
(
  select TEST_ACCOUNT.ACCOUNT_ID as ACCOUNT_ID, TEST_ACCOUNT.ACCOUNT_COUNTRY as ACCOUNT_COUNTRY, TEST_CATEGORY_GROUPINGS.LEAF_CATEG_NAME as LEAF_CATEG_NAME
  FROM TEST_ACCOUNT
  LEFT JOIN TEST_CATEGORY_GROUPINGS
  ON TEST_CATEGORY_GROUPINGS.SITE_ID > 2
) TEST_ACC
ON TEST_KYLIN_FACT.SELLER_ID = TEST_ACC.ACCOUNT_ID
GROUP BY TEST_ACC.LEAF_CATEG_NAME
ORDER BY 1,2,3
LIMIT 10000

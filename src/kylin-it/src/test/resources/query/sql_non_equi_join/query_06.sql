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
-- join with filter
SELECT TEST_ACCOUNT.ACCOUNT_SELLER_LEVEL, COUNT(TEST_COUNTRY.COUNTRY), SUM(TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL)
FROM TEST_ACCOUNT
LEFT JOIN
(
  SELECT *
  FROM TEST_COUNTRY
  WHERE TEST_COUNTRY.LATITUDE > 10.2
) TEST_COUNTRY
ON
TEST_ACCOUNT.ACCOUNT_COUNTRY = TEST_COUNTRY.COUNTRY
AND TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL > 3
GROUP BY TEST_ACCOUNT.ACCOUNT_SELLER_LEVEL
ORDER BY 1,2,3
LIMIT 10000

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
-- Unless required by applicable law or agreed to in writing, softwarea
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- count distinct expr - with filter

SELECT CAL_DT, ACCOUNT_COUNTRY, COUNT(DISTINCT(CASE WHEN ACCOUNT_COUNTRY='US' THEN PRICE ELSE NULL END))
 FROM
 TEST_KYLIN_FACT
 INNER JOIN TEST_ACCOUNT
 ON SELLER_ID = ACCOUNT_ID
 WHERE CAL_DT >=DATE'2012-01-01' AND CAL_DT < DATE'2012-02-01' AND ACCOUNT_COUNTRY IN ('US', 'CN')
 GROUP BY CAL_DT, ACCOUNT_COUNTRY
 ORDER BY CAL_DT, ACCOUNT_COUNTRY


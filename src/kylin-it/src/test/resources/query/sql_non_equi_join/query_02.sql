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
-- test operators
SELECT COUNT(1), TEST_KYLIN_FACT.SELLER_ID, TEST_ACCOUNT.ACCOUNT_ID, sum(TEST_KYLIN_FACT.ITEM_COUNT), count(TEST_ACCOUNT.ACCOUNT_CONTACT)
FROM TEST_KYLIN_FACT
LEFT JOIN TEST_ACCOUNT
ON
TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID
-- + - * / %
AND TEST_KYLIN_FACT.ITEM_COUNT + TEST_KYLIN_FACT.PRICE <= TEST_KYLIN_FACT.ITEM_COUNT * TEST_KYLIN_FACT.PRICE
AND TEST_KYLIN_FACT.ITEM_COUNT - 1 >= 2
AND TEST_KYLIN_FACT.PRICE / TEST_KYLIN_FACT.ITEM_COUNT > 10
AND MOD(TEST_KYLIN_FACT.ITEM_COUNT, 2) = 1
AND TEST_KYLIN_FACT.CAL_DT BETWEEN '2013-05-01' AND DATE '2013-08-01' -- between
AND (case when TEST_KYLIN_FACT.ITEM_COUNT > 100 then 0 else null end) = 0 -- case when
or TEST_ACCOUNT.ACCOUNT_CONTACT is null -- is null
or TEST_ACCOUNT.ACCOUNT_CONTACT is not null -- is not null
and TEST_KYLIN_FACT.LSTG_FORMAT_NAME like '%FP-GT%' -- like
and extract(month from TEST_KYLIN_FACT.CAL_DT) > 5 -- extract
-- cast, trim, substring, length
AND cast(TEST_KYLIN_FACT.PRICE as int) > 10
AND trim(TEST_KYLIN_FACT.LSTG_FORMAT_NAME) = 'FP-GTC'
AND SUBSTRING(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, 1) = 'P-GTC'
AND LENGTH(TEST_KYLIN_FACT.LSTG_FORMAT_NAME) > 5
OR CEIL(TEST_KYLIN_FACT.PRICE) > 50
OR FLOOR(TEST_KYLIN_FACT.PRICE) < 50
OR ABS(TEST_KYLIN_FACT.PRICE) > 50
OR ROUND(TEST_KYLIN_FACT.PRICE, 0) > 50
group by TEST_KYLIN_FACT.SELLER_ID, TEST_ACCOUNT.ACCOUNT_ID
order by 1,2,3,4,5
limit 10000

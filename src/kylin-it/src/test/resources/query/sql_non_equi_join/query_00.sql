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
-- test on join cond supported data types
SELECT COUNT(1), TEST_KYLIN_FACT.SELLER_ID, TEST_ACCOUNT.ACCOUNT_ID, sum(TEST_KYLIN_FACT.ITEM_COUNT), count(TEST_ACCOUNT.ACCOUNT_CONTACT)
FROM TEST_KYLIN_FACT
LEFT JOIN TEST_ACCOUNT
ON
TEST_KYLIN_FACT.CAL_DT BETWEEN '2013-05-01' AND DATE '2013-08-01' -- DATE
AND CURRENT_TIMESTAMP > TIMESTAMP'2018-10-10 11:11:11' -- TIMESTAMP
AND TEST_KYLIN_FACT.LSTG_FORMAT_NAME='FP-GTC' -- STRING
AND TEST_KYLIN_FACT.PRICE > 100 --DECIMAL
AND TEST_KYLIN_FACT.ITEM_COUNT > 2 -- int
AND TEST_KYLIN_FACT.SLR_SEGMENT_CD = 16-- smallint
AND TEST_KYLIN_FACT.LEAF_CATEG_ID > 95672 -- bigint
AND TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID -- int
AND TEST_ACCOUNT.ACCOUNT_CONTACT <> null -- null
group by TEST_KYLIN_FACT.SELLER_ID, TEST_ACCOUNT.ACCOUNT_ID
order by 1,2,3,4,5
limit 10000

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
-- AND OR NOT
TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID
AND TEST_KYLIN_FACT.CAL_DT BETWEEN '2013-05-01' AND DATE '2013-08-01'
OR TEST_KYLIN_FACT.LSTG_FORMAT_NAME='FP-GTC'
OR (NOT (TEST_KYLIN_FACT.PRICE > 100))
-- <> > >= < <=
AND TEST_KYLIN_FACT.ITEM_COUNT >= 2
AND TEST_KYLIN_FACT.LEAF_CATEG_ID < 95672
AND TEST_KYLIN_FACT.SLR_SEGMENT_CD <= 16
AND TEST_KYLIN_FACT.SELLER_ID <> 10000000
-- in, not in
OR TEST_KYLIN_FACT.LSTG_FORMAT_NAME in ('ABIN', 'Auction')
AND TEST_KYLIN_FACT.LSTG_FORMAT_NAME not in ('Others')
group by TEST_KYLIN_FACT.SELLER_ID, TEST_ACCOUNT.ACCOUNT_ID
order by 1,2,3,4,5
limit 10000

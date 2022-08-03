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
select TEST_KYLIN_FACT.LEAF_CATEG_ID, count(TEST_KYLIN_FACT.LEAF_CATEG_ID) as cnt
 from TEST_COUNTRY
 RIGHT JOIN (
   select TEST_ACCOUNT.ACCOUNT_COUNTRY, TEST_KYLIN_FACT.SELLER_ID, TEST_KYLIN_FACT.LEAF_CATEG_ID
   from TEST_ACCOUNT
   RIGHT JOIN TEST_KYLIN_FACT ON TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID
 ) TEST_KYLIN_FACT ON TEST_KYLIN_FACT.ACCOUNT_COUNTRY = TEST_COUNTRY.COUNTRY
 group by TEST_KYLIN_FACT.LEAF_CATEG_ID
 order by TEST_KYLIN_FACT.LEAF_CATEG_ID asc
LIMIT 20

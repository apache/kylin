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
select TEST_ORDER.ORDER_ID,BUYER_ID,TEST_KYLIN_FACT_2.LSTG_SITE_ID,TEST_KYLIN_FACT_2.LSTG_FORMAT_NAME from "DEFAULT".TEST_ORDER
left join "DEFAULT".TEST_KYLIN_FACT on TEST_ORDER.ORDER_ID=TEST_KYLIN_FACT.ORDER_ID
inner join "DEFAULT".TEST_KYLIN_FACT as TEST_KYLIN_FACT_2 on TEST_ORDER.BUYER_ID=TEST_KYLIN_FACT_2.SELLER_ID
group by TEST_ORDER.ORDER_ID,BUYER_ID,TEST_KYLIN_FACT_2.LSTG_SITE_ID,TEST_KYLIN_FACT_2.LSTG_FORMAT_NAME
order by TEST_ORDER.ORDER_ID,BUYER_ID,TEST_KYLIN_FACT_2.LSTG_SITE_ID,TEST_KYLIN_FACT_2.LSTG_FORMAT_NAME
limit 15000

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

select sum(ACCOUNT_SELLER_LEVEL + item_count)
from TEST_ACCOUNT inner JOIN
(
select count(1), ITEM_COUNT item_count, seller_id seller_id from TEST_KYLIN_FACT group by item_count, seller_id
) FACT
on TEST_ACCOUNT.account_id = FACT.seller_id

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

select
  "buyer_account"."ACCOUNT_COUNTRY" as b_country
from
  "DEFAULT"."TEST_ACCOUNT" buyer_account
  inner join "DEFAULT"."TEST_COUNTRY" buyer_country on buyer_account.ACCOUNT_COUNTRY = buyer_country.country
  inner join "DEFAULT"."TEST_ACCOUNT" seller_account on buyer_account.account_country = seller_account.account_country
order by b_country
LIMIT
  500

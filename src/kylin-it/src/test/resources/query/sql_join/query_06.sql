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
select count(T.ACCOUNT_COUNTRY), ACCOUNT_SELLER_LEVEL
 from (
 select ACCOUNT_COUNTRY, ACCOUNT_SELLER_LEVEL
 from TEST_ACCOUNT
 order by ACCOUNT_COUNTRY desc
 ) as T
 inner join TEST_COUNTRY E
 on T.ACCOUNT_COUNTRY = E.COUNTRY
group by ACCOUNT_SELLER_LEVEL

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
sum(t.a1 * 2) EXPR,cast(avg(PRI*2) as bigint) PRI, cast(var_pop(PRI) as bigint) PPP
from (
select
sum(PRICE) as a1, sum(ITEM_COUNT) as a2 ,avg(ITEM_COUNT) PRI
from TEST_KYLIN_FACT
) t
union all
select
sum(t.a1 * 2) EXPR,cast(avg(PRI*2) as bigint) PRI, cast(var_pop(PRI) as bigint) PPP
from (
select min(PRICE) as MIN_PRI,1 as a0,sum(PRICE) as a1, sum(ITEM_COUNT) as a2 ,avg(ITEM_COUNT) PRI
from TEST_KYLIN_FACT
) t



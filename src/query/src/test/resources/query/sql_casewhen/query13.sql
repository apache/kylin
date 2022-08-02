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
-- #ISSUE #12798 KapAggregateRel's columnRowType is wrongly built when inner column is inside the aggregateRel

select x."year",y."year1"
from
(
    select distinct
        CASE WHEN a.LSTG_FORMAT_NAME = 'Others'
        then month(timestampadd(DAY,SELLER_ID-10000000,b.CAL_DT))
        else month(timestampadd(DAY,LSTG_SITE_ID,b.CAL_DT))
        end as "year"
    from TEST_KYLIN_FACT a inner join EDW.TEST_CAL_DT b
    ON a.cal_dt = b.cal_dt
    where a.order_id = 345
)x,
(
    select distinct
        CASE WHEN a.LSTG_FORMAT_NAME = 'Others'
        then b.CAL_DT
        else timestampadd(DAY,1,b.CAL_DT)
        end as "year1"
    from TEST_KYLIN_FACT a inner join EDW.TEST_CAL_DT b
    ON a.cal_dt = b.cal_dt
    where a.order_id = 345
)y
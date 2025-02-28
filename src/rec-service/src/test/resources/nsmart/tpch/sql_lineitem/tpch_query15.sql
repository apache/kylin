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
--  Find the most profitable supplier within given date range.

with revenue_cached as
(
    select
        s_suppkey,
        s_name,
        s_address,
        s_phone,
        sum(l_saleprice) as total_revenue
    from
        v_lineitem
        inner join supplier on s_suppkey=l_suppkey
    where
        l_shipdate >= '1996-01-01'
        and l_shipdate < '1996-04-01'
    group by s_suppkey,s_name,s_address,s_phone
),
max_revenue_cached as
(
    select
        max(total_revenue) as max_revenue
    from
        revenue_cached
)

select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    revenue_cached
    inner join max_revenue_cached on total_revenue = max_revenue
order by s_suppkey;


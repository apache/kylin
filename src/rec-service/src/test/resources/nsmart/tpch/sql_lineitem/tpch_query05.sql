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
--  Sum revenue, filter by date range, filter by region

select
    sn.n_name,
    sum(l_saleprice) as revenue
from
    v_lineitem
    inner join v_orders on l_orderkey = o_orderkey
    inner join customer on o_custkey = c_custkey
    inner join nation cn on c_nationkey = cn.n_nationkey
    inner join supplier on l_suppkey = s_suppkey
    inner join nation sn on s_nationkey = sn.n_nationkey
    inner join region on sn.n_regionkey = r_regionkey
where
    r_name = 'AFRICA'
    and cn.n_name = sn.n_name
    and o_orderdate >= '1993-01-01'
    and o_orderdate < '1994-01-01'
group by
    sn.n_name
order by
    revenue desc;

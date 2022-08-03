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

with tmp1 as (
    select p_partkey from tpch.part where p_name like 'forest%'
),
tmp2 as (
    select s_name, s_address, s_suppkey
    from tpch.supplier, tpch.nation
    where s_nationkey = n_nationkey
    and n_name = 'CANADA'
),
tmp3 as (
    select l_partkey, 0.5 * sum(l_quantity) as sum_quantity, l_suppkey
    from tpch.lineitem, tmp2
    where l_shipdate >= '1994-01-01' and l_shipdate <= '1995-01-01'
    and l_suppkey = s_suppkey 
    group by l_partkey, l_suppkey
),
tmp4 as (
    select ps_partkey, ps_suppkey, ps_availqty
    from tpch.partsupp 
    where ps_partkey IN (select p_partkey from tmp1)
),
tmp5 as (
select
    ps_suppkey
from
    tmp4, tmp3
where
    ps_partkey = l_partkey
    and ps_suppkey = l_suppkey
    and ps_availqty > sum_quantity
)
select
    s_name,
    s_address
from
    tpch.supplier
where
    s_suppkey IN (select ps_suppkey from tmp5)
order by s_name;

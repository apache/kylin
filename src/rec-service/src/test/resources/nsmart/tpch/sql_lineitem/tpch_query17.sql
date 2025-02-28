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
--  Filter by some part, filter by l_quantity < t_avg_quantity, sum up price

with q17_avg as (
    select
        l_partkey,
        0.2 * avg(l_quantity) as t_avg_quantity
    from
        v_lineitem
        inner join part on l_partkey = p_partkey
    where
        p_brand = 'Brand#23'
        and p_container = 'MED BOX'
    group by
        l_partkey
)

select cast(sum(l_extendedprice) / 7.0 as decimal(32,2)) as avg_yearly
from
    v_lineitem
    inner join part on l_partkey = p_partkey
    inner join q17_avg on q17_avg.l_partkey = v_lineitem.l_partkey
where
    p_brand = 'Brand#23'
    and p_container = 'MED BOX'
    and l_quantity < t_avg_quantity;

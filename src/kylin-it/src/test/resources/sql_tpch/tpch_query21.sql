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

-- This query identifies certain suppliers who were not able to ship required parts in a timely manner.
-- Count distinct suppkey from lineitem, filter by order status, receiptdeplayed, nation name

select s_name, count(*) as numwait
from
(
    select
        l1.l_suppkey,
        s_name,
        l1.l_orderkey
    from
        tpch.lineitem l1
        inner join tpch.orders on l1.l_orderkey = o_orderkey
        inner join tpch.supplier on l1.l_suppkey = s_suppkey
        inner join tpch.nation on s_nationkey = n_nationkey
        inner join (
            select
                l_orderkey,
                count (distinct l_suppkey)
            from
                tpch.lineitem inner join tpch.orders on l_orderkey = o_orderkey
            where
                o_orderstatus = 'F'
            group by
                l_orderkey
            having
                count (distinct l_suppkey) > 1
        ) l2 on l1.l_orderkey = l2.l_orderkey
        inner join (
            select
                l_orderkey,
                count (distinct l_suppkey)
            from
                tpch.lineitem inner join tpch.orders on l_orderkey = o_orderkey
            where
                o_orderstatus = 'F'
                and l_receiptdate > l_commitdate
            group by
                l_orderkey
            having
                count (distinct l_suppkey) = 1
        ) l3 on l1.l_orderkey = l3.l_orderkey
    where
        o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and n_name = 'SAUDI ARABIA'
    group by
        l1.l_suppkey,
        s_name,
        l1.l_orderkey
)
group by
    s_name
order by
    numwait desc,
    s_name
limit 100

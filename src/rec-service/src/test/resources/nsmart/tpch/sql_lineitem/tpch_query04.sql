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
--  Count number of delayed orders, filter by date range, group by orderpriority

select
    o_orderpriority,
    count(*) as order_count
from
    (
        select
            l_orderkey,
            o_orderpriority
        from
            v_lineitem
            inner join v_orders on l_orderkey = o_orderkey
        where
            o_orderdate >= '1996-05-01'
            and o_orderdate < '1996-08-01'
            and l_receiptdelayed = 1
        group by
            l_orderkey,
            o_orderpriority
    ) t
group by
    t.o_orderpriority
order by
    t.o_orderpriority;

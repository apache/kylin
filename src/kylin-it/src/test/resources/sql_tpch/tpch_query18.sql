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

with q18_tmp_cached as (
select
	l_orderkey,
	sum(l_quantity) as t_sum_quantity
from
	tpch.lineitem
where
	l_orderkey is not null
group by
	l_orderkey
)
select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity)
from
	tpch.customer,
	tpch.orders,
	q18_tmp_cached t,
	tpch.lineitem l
where
	c_custkey = o_custkey
	and o_orderkey = t.l_orderkey
	and o_orderkey is not null
	and t.t_sum_quantity > 300
	and o_orderkey = l.l_orderkey
	and l.l_orderkey is not null
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate 
limit 100;

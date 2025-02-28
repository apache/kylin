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
-- Sum revenue from lineitem, filter by shipdate range, nation name, group by supplier nation, customer nation and year.

select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			l_shipyear as l_year,
			l_saleprice as volume
		from
			v_lineitem
			inner join supplier on s_suppkey = l_suppkey
			inner join v_orders on l_orderkey = o_orderkey
			inner join customer on o_custkey = c_custkey
			inner join nation n1 on s_nationkey = n1.n_nationkey
			inner join nation n2 on c_nationkey = n2.n_nationkey
		where
			(
				(n1.n_name = 'KENYA' and n2.n_name = 'PERU')
				or (n1.n_name = 'PERU' and n2.n_name = 'KENYA')
			)
			and l_shipdate between '1995-01-01' and '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year;

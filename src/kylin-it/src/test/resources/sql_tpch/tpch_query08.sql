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
	o_year,
	sum(peru_volumn) / sum(volume) as mkt_share
from
	(
		select
			year(o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			case
				when n2.n_name = 'PERU' then l_extendedprice * (1 - l_discount)
				else 0
			end as peru_volumn
		from
	    	tpch.lineitem
		    inner join tpch.part on l_partkey = p_partkey
		    inner join tpch.supplier on l_suppkey = s_suppkey
			inner join tpch.orders on l_orderkey = o_orderkey
			inner join tpch.customer on o_custkey = c_custkey
		    inner join tpch.nation n1 on c_nationkey = n1.n_nationkey
		    inner join tpch.nation n2 on s_nationkey = n2.n_nationkey
		    inner join tpch.region on n1.n_regionkey = r_regionkey
		where
			r_name = 'AMERICA'
			and o_orderdate between '1995-01-01' and '1996-12-31'
			and p_type = 'ECONOMY BURNISHED NICKEL'
	) as all_nations
group by
	o_year
order by
	o_year;

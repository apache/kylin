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

with q22_customer_tmp_cached as (
select
	c_acctbal,
	c_custkey,
	substring(c_phone, 1, 2) as cntrycode
from
	tpch.customer
where
	substring(c_phone, 1, 2) = '13' or
	substring(c_phone, 1, 2) = '31' or
	substring(c_phone, 1, 2) = '23' or
	substring(c_phone, 1, 2) = '29' or
	substring(c_phone, 1, 2) = '30' or
	substring(c_phone, 1, 2) = '18' or
	substring(c_phone, 1, 2) = '17'
 ),

q22_customer_tmp1_cached as (
select
	avg(c_acctbal) as avg_acctbal
from
	q22_customer_tmp_cached
where
	c_acctbal > 0.00
),
q22_orders_tmp_cached as (
select
	o_custkey
from
	tpch.orders
group by
	o_custkey
)

select
	cntrycode,
	count(1) as numcust,
	sum(c_acctbal) as totacctbal
from (
	select
		cntrycode,
		c_acctbal,
		avg_acctbal
	from
		q22_customer_tmp1_cached ct1, (
			select
				cntrycode,
				c_acctbal
			from
				q22_orders_tmp_cached ot
				right outer join q22_customer_tmp_cached ct
				on ct.c_custkey = ot.o_custkey
			where
				o_custkey is null
		) ct2
) as a
where
	c_acctbal > avg_acctbal
group by
	cntrycode
order by
	cntrycode;

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
-- The Query counts, by ship mode, for lineitems actually received by customers in a given year,
-- the number of lineitems belonging to orders for which the l_receiptdate exceeds the l_commitdate for
-- two different specified ship modes. Only lineitems that were actually shipped before the l_commitdate are con- sidered.
-- The late lineitems are partitioned into two groups, those with priority URGENT or HIGH,
-- and those with a priority other than URGENT or HIGH.

with in_scope_data as(
	select
		l_shipmode,
		o_orderpriority
	from
		v_lineitem inner join v_orders on l_orderkey = o_orderkey
	where
		l_shipmode in ('REG AIR', 'MAIL')
		and l_receiptdelayed = 1
		and l_shipdelayed = 0
		and l_receiptdate >= '1995-01-01'
		and l_receiptdate < '1996-01-01'
), all_l_shipmode as(
	select
		distinct l_shipmode
	from
		in_scope_data
), high_line as(
	select
		l_shipmode,
		count(*) as high_line_count
	from
		in_scope_data
	where
		o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH'
	group by l_shipmode
), low_line as(
	select
		l_shipmode,
		count(*) as low_line_count
	from
		in_scope_data
	where
		o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH'
	group by l_shipmode
)
select
	al.l_shipmode, hl.high_line_count, ll.low_line_count
from
	all_l_shipmode al
	left join high_line hl on al.l_shipmode = hl.l_shipmode
	left join low_line ll on al.l_shipmode = ll.l_shipmode
order by
	al.l_shipmode;


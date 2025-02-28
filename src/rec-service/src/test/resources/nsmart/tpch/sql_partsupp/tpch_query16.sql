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
-- This query finds out how many suppliers can supply parts with given attributes.
-- It might be used, for example, to determine whether there is a sufficient number of suppliers for heavily ordered parts.
--
-- Count distinct supplier from partsupp, filter by supplier comment, part brand/type/size, group by part brand/type/size
--
-- Hive result is incorrect on HDP 2.4 sandbox.

select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	v_partsupp
	inner join part on p_partkey = ps_partkey
	inner join (
		select
			s_suppkey
		from
			supplier
		where
			s_comment not like '%Customer%Complaints%'
	) on ps_suppkey = s_suppkey
where
    p_brand <> 'Brand#34'
	and p_type not like 'ECONOMY BRUSHED%'
	and p_size in (22, 14, 27, 49, 21, 33, 35, 28)
group by
    p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size;

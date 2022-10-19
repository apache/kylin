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


-- ISSUE #2310
-- from KE sql_sparder_only/query01.sql
-- commit e3f35c3

select
count(distinct CASE WHEN xx > 0 THEN CAST(ORDER_ID AS VARCHAR(256)) || LSTG_FORMAT_NAME else null end) as DIST_CNT
from
(
	select
	ORDER_ID,
	LSTG_FORMAT_NAME,
	sum(1) as xx
	from TEST_KYLIN_FACT
	group by ORDER_ID, LSTG_FORMAT_NAME
)

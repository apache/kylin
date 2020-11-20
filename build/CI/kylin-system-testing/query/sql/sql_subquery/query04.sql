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

select fact.part_dt, sum(fact.price) as sum_price, count(1) as cnt_1
from KYLIN_SALES fact
 inner JOIN KYLIN_CAL_DT as KYLIN_CAL_DT
 ON fact.part_dt = KYLIN_CAL_DT.cal_dt
 inner JOIN kylin_category_groupings
 ON fact.leaf_categ_id = kylin_category_groupings.leaf_categ_id AND fact.lstg_site_id = kylin_category_groupings.site_id
inner join
(
	select KYLIN_SALES.PART_DT, max(KYLIN_SALES.PART_DT) as mmm from KYLIN_SALES  inner JOIN KYLIN_CAL_DT as KYLIN_CAL_DT
 ON KYLIN_SALES.PART_DT = KYLIN_CAL_DT.cal_dt
 inner JOIN kylin_category_groupings
 ON KYLIN_SALES.leaf_categ_id = kylin_category_groupings.leaf_categ_id AND KYLIN_SALES.lstg_site_id = kylin_category_groupings.site_id
 group by KYLIN_SALES.PART_DT order by 2 desc limit 7
) cal_2 on fact.part_dt = cal_2.mmm
group by fact.part_dt
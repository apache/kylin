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
week_beg_dt as week,
intersect_count(TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['FP-GTC']) as a_cnt,
intersect_value(TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['FP-GTC']) as a_value,
intersect_value(TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['FP-GTC', 'Auction']) as ab,
intersect_value(TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['FP-GTC|Auction', 'Others']) as a_or_b_and_c,
count(distinct TEST_COUNT_DISTINCT_BITMAP) as sellers
from test_kylin_fact left join edw.test_cal_dt on test_kylin_fact.cal_dt = edw.test_cal_dt.CAL_DT
where week_beg_dt in (DATE '2013-12-22', DATE '2012-06-23')
group by week_beg_dt
;{"scanRowCount":10018,"scanBytes":0,"scanFiles":2,"cuboidId":[276480]}
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
intersect_count( TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['FP-GTC']) as a,
intersect_count( TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['Auction']) as b,
intersect_count( TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['Others']) as c,
intersect_count( TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['FP-GTC', 'Auction']) as ab,
intersect_count( TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['FP-GTC', 'Others']) as ac,
intersect_count( TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['FP-GTC', 'Auction', 'Others']) as abc,
count(distinct TEST_COUNT_DISTINCT_BITMAP) as sellers,
count(*) as cnt
from test_kylin_fact left join edw.test_cal_dt on test_kylin_fact.cal_dt = edw.test_cal_dt.CAL_DT
where week_beg_dt in (DATE '2013-12-22', DATE '2012-06-23')
group by week_beg_dt

